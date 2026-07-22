"""Import issuer-published BITB BTC addresses without retaining signed URLs.

The Bitwise BITB public page is a dynamic HTML document and can include
short-lived signed report links. The importer parses it in memory and persists
only a canonical, non-sensitive address snapshot plus the source-page hash.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from typing import Any

import httpx

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.evidence import EvidenceInput


BITWISE_BITB_PUBLIC_URL = "https://bitbetf.com/"
BITWISE_BITB_ENTITY_ID = "official:bitwise_bitb"
BITWISE_BITB_ENTITY_NAME = "Bitwise Bitcoin ETF (BITB)"
BITWISE_BITB_IMPORT_METHOD = "direct_issuer_publication"
_MAX_PAGE_BYTES = 2 * 1024 * 1024
_EVIDENCE_TTL_DAYS = 31


class BitwiseBitbEvidenceError(ValueError):
    """Raised when the public BITB address page cannot be safely parsed."""


@dataclass(frozen=True)
class BitwiseBitbSnapshot:
    """Sanitized issuer address snapshot ready for restricted raw storage."""

    source_url: str
    source_page_sha256: str
    retrieved_at: datetime
    reported_updated_at: datetime
    addresses: tuple[str, ...]

    def safe_payload(self) -> bytes:
        """Return only durable, non-sensitive fields from the issuer page."""

        payload = {
            "schema_version": "bitwise_bitb_public_wallet_snapshot_v1",
            "source_url": self.source_url,
            "source_page_sha256": self.source_page_sha256,
            "retrieved_at": _format_utc(self.retrieved_at),
            "reported_updated_at": _format_utc(self.reported_updated_at),
            "active_addresses": list(self.addresses),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def fetch_bitwise_bitb_snapshot(
    client: httpx.Client, *, retrieved_at: datetime | None = None
) -> BitwiseBitbSnapshot:
    """Fetch the fixed issuer URL and return a sanitized wallet snapshot."""

    try:
        response = client.get(
            BITWISE_BITB_PUBLIC_URL,
            headers={"Accept": "text/html", "User-Agent": "crypto-address-identity/0.1"},
        )
    except httpx.HTTPError as exc:
        raise BitwiseBitbEvidenceError("Unable to retrieve the BITB public page") from exc
    if response.status_code != 200:
        raise BitwiseBitbEvidenceError("BITB public page returned an unexpected status")
    if len(response.content) > _MAX_PAGE_BYTES:
        raise BitwiseBitbEvidenceError("BITB public page exceeds the configured input limit")
    return parse_bitwise_bitb_page(
        response.content,
        retrieved_at=retrieved_at or datetime.now(UTC),
    )


def parse_bitwise_bitb_page(page_payload: bytes, *, retrieved_at: datetime) -> BitwiseBitbSnapshot:
    """Extract active BTC addresses from a BITB issuer page in memory only."""

    if retrieved_at.tzinfo is None:
        raise BitwiseBitbEvidenceError("retrieved_at must be timezone-aware")
    if not page_payload or len(page_payload) > _MAX_PAGE_BYTES:
        raise BitwiseBitbEvidenceError("BITB public page has an invalid size")
    try:
        document = _next_data(page_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BitwiseBitbEvidenceError("BITB public page has invalid embedded JSON") from exc
    wallets = _wallets_payload(document)
    updated_at = _parse_timestamp(wallets.get("updatedAt"))
    raw_addresses = wallets.get("walletBalances")
    if not isinstance(raw_addresses, list) or not raw_addresses:
        raise BitwiseBitbEvidenceError("BITB public page has no wallet addresses")

    addresses: set[str] = set()
    for row in raw_addresses:
        if not isinstance(row, dict) or not isinstance(row.get("active"), bool):
            raise BitwiseBitbEvidenceError("BITB public page has malformed wallet data")
        if not row["active"]:
            continue
        address = row.get("address")
        if not isinstance(address, str):
            raise BitwiseBitbEvidenceError("BITB public page has a wallet without an address")
        try:
            addresses.add(normalize_bitcoin_address(address).normalized_address)
        except ValueError as exc:
            raise BitwiseBitbEvidenceError("BITB public page has an invalid BTC address") from exc
    if not addresses:
        raise BitwiseBitbEvidenceError("BITB public page has no active wallet addresses")

    return BitwiseBitbSnapshot(
        source_url=BITWISE_BITB_PUBLIC_URL,
        source_page_sha256=hashlib.sha256(page_payload).hexdigest(),
        retrieved_at=retrieved_at.astimezone(UTC),
        reported_updated_at=updated_at,
        addresses=tuple(sorted(addresses)),
    )


def official_bitwise_evidence_records(
    snapshot: BitwiseBitbSnapshot, *, artifact_sha256: str
) -> list[EvidenceInput]:
    """Create Tier-B issuer-publication evidence with short freshness bounds."""

    expires_at = snapshot.reported_updated_at + timedelta(days=_EVIDENCE_TTL_DAYS)
    return [
        EvidenceInput.model_validate(
            {
                "chain_key": "bitcoin",
                "address": address,
                "assertion_type": "entity_control",
                "candidate_entity_id": BITWISE_BITB_ENTITY_ID,
                "candidate_entity_name": BITWISE_BITB_ENTITY_NAME,
                "source_authority": "official",
                "evidence_tier": "B",
                "verification_method": BITWISE_BITB_IMPORT_METHOD,
                "source_url": snapshot.source_url,
                "artifact_sha256": artifact_sha256,
                "license_ref": "Bitwise BITB public wallet disclosure",
                "independence_group": "bitwise_bitb_public_wallets",
                "asserted_at": snapshot.reported_updated_at,
                "observed_at": snapshot.retrieved_at,
                "expires_at": expires_at,
                "evidence_status": "valid",
                "imported_by": "bitwise_bitb_public_import",
            }
        )
        for address in snapshot.addresses
    ]


class _NextDataExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self._inside_next_data = False
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "script" and dict(attrs).get("id") == "__NEXT_DATA__":
            self._inside_next_data = True

    def handle_data(self, data: str) -> None:
        if self._inside_next_data:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._inside_next_data:
            self._inside_next_data = False

    @property
    def text(self) -> str:
        return "".join(self._parts)


def _next_data(document: str) -> dict[str, Any]:
    parser = _NextDataExtractor()
    parser.feed(document)
    parser.close()
    if not parser.text:
        raise BitwiseBitbEvidenceError("BITB public page has no Next data payload")
    payload = json.loads(parser.text)
    if not isinstance(payload, dict):
        raise BitwiseBitbEvidenceError("BITB Next data payload is malformed")
    return payload


def _wallets_payload(document: dict[str, Any]) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []

    def visit(value: object) -> None:
        if isinstance(value, dict):
            if isinstance(value.get("walletBalances"), list):
                matches.append(value)
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(document)
    if len(matches) != 1:
        raise BitwiseBitbEvidenceError("BITB public page has ambiguous wallet data")
    return matches[0]


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise BitwiseBitbEvidenceError("BITB public page has no wallet update timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BitwiseBitbEvidenceError("BITB public page has an invalid update timestamp") from exc
    if parsed.tzinfo is None:
        raise BitwiseBitbEvidenceError("BITB public page update timestamp lacks a timezone")
    return parsed.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
