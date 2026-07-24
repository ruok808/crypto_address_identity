"""0xRouter address-enriched client and conservative Bitcoin parser."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import quote

import httpx

from crypto_address_identity.chains.bitcoin import BitcoinAddressError, BitcoinAddressSubject, normalize_bitcoin_address
from crypto_address_identity.core.config import Settings


class ProviderTokenMissing(RuntimeError):
    """Raised before execute-mode dispatch without an in-memory provider token."""


class ProviderPayloadError(ValueError):
    """Raised for a syntactically valid response that violates the BTC contract."""


class ProviderProfile(StrEnum):
    DISCOVERY = "discovery"
    DETAIL = "detail"


@dataclass(frozen=True)
class ProviderFetchResult:
    http_status: int | None
    outcome: str
    body: bytes


@dataclass(frozen=True)
class ProviderEvidenceCandidate:
    assertion_type: Literal["entity_control", "address_label"]
    candidate_entity_id: str | None
    candidate_entity_name: str | None
    candidate_label: str | None
    candidate_wallet_role: None
    provider_entity_id: str | None
    provider_tag_id: str | None
    evidence_tier: Literal["C"]


@dataclass(frozen=True)
class ParsedBitcoinResponse:
    evidence_candidates: tuple[ProviderEvidenceCandidate, ...]
    extension_metadata: dict[str, object]
    schema_fingerprint: str


class ZeroXRouterClient:
    """Narrow synchronous client; callers provide the external rate reservation."""

    def __init__(self, settings: Settings, *, transport: httpx.BaseTransport | None = None) -> None:
        self.settings = settings
        self._client = httpx.Client(
            transport=transport,
            timeout=settings.http_timeout_seconds,
            follow_redirects=False,
        )

    def close(self) -> None:
        self._client.close()

    def build_request(self, address: str, profile: ProviderProfile) -> httpx.Request:
        subject = normalize_bitcoin_address(address)
        path = "/chaindata/intelligence/address_enriched/" + quote(
            subject.normalized_address, safe=""
        ) + "/all"
        include_tags = "true" if profile is ProviderProfile.DETAIL else "false"
        params = {
            "includeTags": include_tags,
            "includeEntityPredictions": "true",
            "includeClusters": "false",
        }
        return self._build_authenticated_request(path, params=params)

    def fetch(self, address: str, profile: ProviderProfile) -> ProviderFetchResult:
        request = self.build_request(address, profile)
        return self.fetch_request(request)

    def build_btc_coverage_enrichment_request(self, address: str) -> httpx.Request:
        """Build the live-validated full BTC identity request.

        The provider's documented ``/all`` request with all three expansions is
        the only address-enriched shape validated against the configured token.
        The response budget and TTL cache bound its cost; callers must not
        substitute an unvalidated smaller request profile.
        """

        subject = normalize_bitcoin_address(address)
        return self._build_authenticated_request(
            "/chaindata/intelligence/address_enriched/"
            + quote(subject.normalized_address, safe="")
            + "/all",
            params={
                "includeTags": "true",
                "includeEntityPredictions": "true",
                "includeClusters": "true",
            },
        )

    def build_entity_request(self, entity_id: str) -> httpx.Request:
        return self._build_authenticated_request(
            "/chaindata/intelligence/entity/" + quote(_validate_entity_id(entity_id), safe="")
        )

    def build_entity_predictions_request(self, entity_id: str) -> httpx.Request:
        return self._build_authenticated_request(
            "/chaindata/intelligence/entity_predictions/"
            + quote(_validate_entity_id(entity_id), safe="")
        )

    def build_entity_balance_changes_request(
        self,
        *,
        entity_types: tuple[str, ...],
        interval: str,
        order_by: str,
        order_dir: str = "desc",
        limit: int = 100,
        offset: int = 0,
    ) -> httpx.Request:
        if not entity_types or any(not _is_safe_query_word(item) for item in entity_types):
            raise ValueError("entity_types must contain safe non-empty values")
        if interval not in {"7d", "14d", "30d"}:
            raise ValueError("unsupported balance-change interval")
        if order_by not in {
            "balanceUsd",
            "balanceUsdChange",
            "balanceUsdPctChange",
            "balanceUnit",
            "balanceUnitChange",
            "balanceUnitPctChange",
        }:
            raise ValueError("unsupported balance-change ordering")
        if order_dir not in {"asc", "desc"}:
            raise ValueError("unsupported balance-change direction")
        if not 1 <= limit <= 100 or offset < 0:
            raise ValueError("limit must be 1..100 and offset must be non-negative")
        return self._build_authenticated_request(
            "/chaindata/intelligence/entity_balance_changes",
            params={
                "chains": "bitcoin",
                "entityTypes": ",".join(entity_types),
                "interval": interval,
                "orderBy": order_by,
                "orderDir": order_dir,
                "limit": str(limit),
                "offset": str(offset),
            },
        )

    def fetch_request(self, request: httpx.Request) -> ProviderFetchResult:
        response: httpx.Response | None = None
        for _ in range(self.settings.max_transport_retries + 1):
            try:
                response = self._client.send(request)
                break
            except httpx.TransportError:
                continue
        if response is None:
            return ProviderFetchResult(None, "transport_error", b"")

        if response.status_code == 429:
            return ProviderFetchResult(response.status_code, "rate_limited", response.content)
        if 200 <= response.status_code < 300:
            return ProviderFetchResult(response.status_code, "success", response.content)
        return ProviderFetchResult(response.status_code, "http_error", response.content)

    def _build_authenticated_request(
        self, path: str, *, params: dict[str, str] | None = None
    ) -> httpx.Request:
        token = self.settings.provider_token_value()
        if token is None:
            raise ProviderTokenMissing("Provider token is not configured")
        return self._client.build_request(
            "GET",
            f"{self.settings.provider_base_url}{path}",
            params=params,
            headers={"X-My-Token": token, "Accept": "application/json"},
        )


def parse_bitcoin_response(payload: bytes, expected_subject: BitcoinAddressSubject) -> ParsedBitcoinResponse:
    """Parse only the Bitcoin branch into Tier C candidate evidence."""

    try:
        decoded = json.loads(payload)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProviderPayloadError("Provider body is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise ProviderPayloadError("Provider body must be a JSON object")
    root = decoded.get("bitcoin")
    if not isinstance(root, dict):
        raise ProviderPayloadError("Provider response has no Bitcoin object")
    if root.get("chain") != "bitcoin":
        raise ProviderPayloadError("Provider Bitcoin root has a chain mismatch")
    raw_address = root.get("address")
    if not isinstance(raw_address, str):
        raise ProviderPayloadError("Provider Bitcoin root has no address")
    try:
        parsed_subject = normalize_bitcoin_address(raw_address)
    except BitcoinAddressError as exc:
        raise ProviderPayloadError("Provider Bitcoin root contains an invalid address") from exc
    if parsed_subject.address_id != expected_subject.address_id:
        raise ProviderPayloadError("Provider Bitcoin root address mismatch")

    candidates: list[ProviderEvidenceCandidate] = []
    entity = root.get("arkhamEntity")
    if entity is not None:
        if not isinstance(entity, dict):
            raise ProviderPayloadError("arkhamEntity must be an object")
        entity_id = entity.get("id")
        entity_name = entity.get("name")
        if not isinstance(entity_id, str) or not entity_id or not isinstance(entity_name, str) or not entity_name:
            raise ProviderPayloadError("arkhamEntity requires id and name")
        candidates.append(
            ProviderEvidenceCandidate(
                assertion_type="entity_control",
                candidate_entity_id=_local_provider_entity_id(entity_id),
                candidate_entity_name=entity_name,
                candidate_label=None,
                candidate_wallet_role=None,
                provider_entity_id=entity_id,
                provider_tag_id=None,
                evidence_tier="C",
            )
        )

    label = root.get("arkhamLabel")
    if label is not None:
        if not isinstance(label, dict):
            raise ProviderPayloadError("arkhamLabel must be an object")
        label_name = label.get("name")
        if not isinstance(label_name, str) or not label_name:
            raise ProviderPayloadError("arkhamLabel requires name")
        candidates.append(
            ProviderEvidenceCandidate(
                assertion_type="address_label",
                candidate_entity_id=None,
                candidate_entity_name=None,
                candidate_label=label_name,
                candidate_wallet_role=None,
                provider_entity_id=None,
                provider_tag_id=None,
                evidence_tier="C",
            )
        )

    tags = root.get("populatedTags")
    if tags is not None:
        if not isinstance(tags, list):
            raise ProviderPayloadError("populatedTags must be a list")
        for tag in tags:
            if not isinstance(tag, dict):
                raise ProviderPayloadError("populatedTags entries must be objects")
            tag_label = tag.get("label")
            tag_id = tag.get("id")
            if not isinstance(tag_label, str) or not tag_label:
                raise ProviderPayloadError("populatedTags entries require label")
            if tag_id is not None and not isinstance(tag_id, str):
                raise ProviderPayloadError("populatedTags id must be a string")
            candidates.append(
                ProviderEvidenceCandidate(
                    assertion_type="address_label",
                    candidate_entity_id=None,
                    candidate_entity_name=None,
                    candidate_label=tag_label,
                    candidate_wallet_role=None,
                    provider_entity_id=None,
                    provider_tag_id=tag_id,
                    evidence_tier="C",
                )
            )

    extensions: dict[str, object] = {}
    if "isUserAddress" in root:
        if not isinstance(root["isUserAddress"], bool):
            raise ProviderPayloadError("isUserAddress must be a boolean")
        extensions["isUserAddress"] = root["isUserAddress"]
    known_keys = {"address", "chain", "arkhamEntity", "arkhamLabel", "populatedTags", "isUserAddress"}
    extensions["unknownKeys"] = tuple(sorted(key for key in root if key not in known_keys))
    return ParsedBitcoinResponse(
        evidence_candidates=tuple(candidates),
        extension_metadata=extensions,
        schema_fingerprint=_schema_fingerprint(decoded),
    )


def _local_provider_entity_id(provider_entity_id: str) -> str:
    return f"arkham:{provider_entity_id}"


def _validate_entity_id(value: str) -> str:
    normalized = value.strip()
    if not normalized or any(char.isspace() for char in normalized):
        raise ValueError("entity identifier must be a compact non-empty value")
    return normalized


def _is_safe_query_word(value: str) -> bool:
    return bool(value) and all(char.isalnum() or char in {"_", "-"} for char in value)


def _schema_fingerprint(value: Any) -> str:
    def shape(item: Any) -> Any:
        if isinstance(item, dict):
            return {str(key): shape(item[key]) for key in sorted(item)}
        if isinstance(item, list):
            return [shape(entry) for entry in item]
        return type(item).__name__

    encoded = json.dumps(shape(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
