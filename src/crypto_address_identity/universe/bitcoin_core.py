"""Read-only Bitcoin Core source capability and finality probe."""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import httpx

from crypto_address_identity.core.config import Settings
from crypto_address_identity.universe.models import SourceProbeResult


_PROBE_SCHEMA_SHA256 = hashlib.sha256(
    json.dumps(
        {
            "methods": (
                "getblockchaininfo",
                "getblockhash",
                "getblockheader",
                "getindexinfo",
            ),
            "version": "bitcoin_core_probe_v1",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
).hexdigest()


class BitcoinCoreRpcError(RuntimeError):
    """Safe RPC boundary error that never contains an upstream payload."""


def _read_cookie(cookie_file: Path) -> tuple[str, str]:
    try:
        value = cookie_file.expanduser().resolve().read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise BitcoinCoreRpcError("Bitcoin Core cookie is unavailable") from exc
    username, separator, password = value.partition(":")
    if not separator or not username or not password or "\n" in value:
        raise BitcoinCoreRpcError("Bitcoin Core cookie is malformed")
    return username, password


class BitcoinCoreRpc:
    """Minimal JSON-RPC client restricted by its probe caller."""

    def __init__(
        self,
        *,
        url: str,
        cookie_file: Path,
        timeout_seconds: int,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        username, password = _read_cookie(cookie_file)
        self._request_ids = itertools.count(1)
        self._client = httpx.Client(
            base_url=url,
            auth=(username, password),
            timeout=timeout_seconds,
            transport=transport,
            trust_env=False,
        )

    def call(self, method: str, params: list[object] | None = None) -> object:
        request_id = next(self._request_ids)
        try:
            response = self._client.post(
                "",
                json={
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params or [],
                },
            )
            response.raise_for_status()
            decoded = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise BitcoinCoreRpcError(
                "Bitcoin Core returned an unavailable RPC result"
            ) from exc
        if (
            not isinstance(decoded, dict)
            or decoded.get("id") != request_id
            or decoded.get("error") is not None
            or "result" not in decoded
        ):
            raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
        return decoded["result"]

    def close(self) -> None:
        self._client.close()


class BitcoinCoreProbe:
    """Inspect source safety without reading blocks or mutating node state."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._transport = transport

    def run(self) -> SourceProbeResult:
        try:
            rpc = BitcoinCoreRpc(
                url=self._settings.bitcoin_rpc_url,
                cookie_file=self._settings.bitcoin_rpc_cookie_file,
                timeout_seconds=self._settings.bitcoin_rpc_timeout_seconds,
                transport=self._transport,
            )
        except BitcoinCoreRpcError:
            return self._blocked("bitcoin_rpc_unavailable")

        try:
            blockchain_info = self._mapping(rpc.call("getblockchaininfo"))
            initial = self._validate_chain_state(blockchain_info)
            if initial is not None:
                return initial

            latest_height = self._integer(blockchain_info, "blocks")
            latest_hash = self._hash(blockchain_info, "bestblockhash")
            finalized_height = latest_height - self._settings.bitcoin_finality_depth
            if finalized_height < 0:
                return self._blocked(
                    "bitcoin_finality_unavailable",
                    latest_height=latest_height,
                    latest_hash=latest_hash,
                )

            finalized_hash_value = rpc.call("getblockhash", [finalized_height])
            if not isinstance(finalized_hash_value, str):
                raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
            finalized_hash = self._validated_hash(finalized_hash_value)
            header = self._mapping(
                rpc.call("getblockheader", [finalized_hash, True])
            )
            indexes = self._mapping(rpc.call("getindexinfo"))

            header_height = self._integer(header, "height")
            confirmations = self._integer(header, "confirmations")
            header_time = self._integer(header, "time")
            if header_height != finalized_height:
                return self._blocked(
                    "bitcoin_finalized_height_mismatch",
                    latest_height=latest_height,
                    latest_hash=latest_hash,
                    finalized_height=finalized_height,
                    finalized_hash=finalized_hash,
                )
            if confirmations < self._settings.bitcoin_finality_depth + 1:
                return self._blocked(
                    "bitcoin_finality_too_shallow",
                    latest_height=latest_height,
                    latest_hash=latest_hash,
                    finalized_height=finalized_height,
                    finalized_hash=finalized_hash,
                )

            pruned = blockchain_info.get("pruned")
            if not isinstance(pruned, bool):
                raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
            capabilities = ["utxo_probe"]
            warnings: list[str] = []
            if not pruned:
                capabilities.append("historical_block_scan")
            else:
                warnings.append("bitcoin_pruned_node")
            tx_index = indexes.get("txindex")
            if isinstance(tx_index, Mapping) and tx_index.get("synced") is True:
                capabilities.append("txindex")

            return SourceProbeResult(
                source_kind="bitcoin_core",
                status="partial" if pruned else "accepted",
                schema_sha256=_PROBE_SCHEMA_SHA256,
                latest_height=latest_height,
                latest_hash=latest_hash,
                latest_time=datetime.fromtimestamp(header_time, tz=UTC),
                finalized_height=finalized_height,
                finalized_hash=finalized_hash,
                dry_run_bytes=None,
                script_completeness=not pruned,
                capabilities=tuple(capabilities),
                warnings=tuple(warnings),
            )
        except (BitcoinCoreRpcError, KeyError, TypeError, ValueError):
            return self._blocked("bitcoin_rpc_rejected")
        finally:
            rpc.close()

    def _validate_chain_state(
        self, info: Mapping[str, object]
    ) -> SourceProbeResult | None:
        if info.get("chain") != "main":
            return self._blocked("bitcoin_network_not_mainnet")
        if info.get("initialblockdownload") is not False:
            return self._blocked("bitcoin_initial_block_download")
        blocks = self._integer(info, "blocks")
        headers = self._integer(info, "headers")
        latest_hash = self._hash(info, "bestblockhash")
        if headers > blocks:
            return self._blocked(
                "bitcoin_headers_ahead_of_blocks",
                latest_height=blocks,
                latest_hash=latest_hash,
            )
        if headers < blocks:
            return self._blocked(
                "bitcoin_headers_behind_blocks",
                latest_height=blocks,
                latest_hash=latest_hash,
            )
        verification_progress = info.get("verificationprogress")
        if not isinstance(verification_progress, (int, float)) or float(
            verification_progress
        ) < 0.999:
            return self._blocked(
                "bitcoin_verification_incomplete",
                latest_height=blocks,
                latest_hash=latest_hash,
            )
        if blocks < self._settings.bitcoin_finality_depth:
            return self._blocked(
                "bitcoin_finality_unavailable",
                latest_height=blocks,
                latest_hash=latest_hash,
            )
        return None

    @staticmethod
    def _mapping(value: object) -> Mapping[str, object]:
        if not isinstance(value, Mapping):
            raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
        return value

    @staticmethod
    def _integer(value: Mapping[str, object], key: str) -> int:
        candidate = value[key]
        if isinstance(candidate, bool) or not isinstance(candidate, int) or candidate < 0:
            raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
        return candidate

    @classmethod
    def _hash(cls, value: Mapping[str, object], key: str) -> str:
        candidate = value[key]
        if not isinstance(candidate, str):
            raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
        return cls._validated_hash(candidate)

    @staticmethod
    def _validated_hash(value: str) -> str:
        if len(value) != 64 or value != value.lower():
            raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
        try:
            bytes.fromhex(value)
        except ValueError as exc:
            raise BitcoinCoreRpcError(
                "Bitcoin Core returned a rejected RPC result"
            ) from exc
        return value

    @staticmethod
    def _blocked(
        reason: str,
        *,
        latest_height: int | None = None,
        latest_hash: str | None = None,
        finalized_height: int | None = None,
        finalized_hash: str | None = None,
    ) -> SourceProbeResult:
        return SourceProbeResult(
            source_kind="bitcoin_core",
            status="blocked",
            schema_sha256=_PROBE_SCHEMA_SHA256,
            latest_height=latest_height,
            latest_hash=latest_hash,
            latest_time=None,
            finalized_height=finalized_height,
            finalized_hash=finalized_hash,
            dry_run_bytes=None,
            script_completeness=False,
            capabilities=(),
            blocking_reasons=(reason,),
        )
