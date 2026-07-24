from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.universe.models import (
    AddressFeatureRow,
    ScriptSubjectRow,
    SourceManifest,
    UniverseCoverageCounters,
)


BTC_ADDRESSES = (
    "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
    "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC",
    "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
)


@pytest.fixture
def universe_source_manifest() -> SourceManifest:
    return SourceManifest(
        campaign_id="btc-20260724",
        source_kind="fixture",
        source_revision="fixture-v1",
        cutoff_height=900_000,
        cutoff_hash="01" * 32,
        cutoff_time=datetime(2026, 7, 24, tzinfo=UTC),
        schema_sha256="02" * 32,
        query_sha256="03" * 32,
        source_capabilities=("address_rows", "script_hex", "source_accounting"),
        script_completeness=True,
    )


def make_feature(
    address: str = BTC_ADDRESSES[0],
    *,
    current_utxo_sats: int = 100,
    lifetime_received_sats: int = 300,
    lifetime_spent_sats: int = 200,
    **updates: object,
) -> AddressFeatureRow:
    subject = normalize_bitcoin_address(address)
    values: dict[str, object] = {
        "address_id": subject.address_id,
        "normalized_address": subject.normalized_address,
        "address_type": subject.address_type,
        "first_seen_height": 100,
        "last_seen_height": 200,
        "first_seen_time": datetime(2025, 1, 1, tzinfo=UTC),
        "last_seen_time": datetime(2026, 1, 1, tzinfo=UTC),
        "output_count": 3,
        "spent_output_count": 2,
        "transaction_count": 4,
        "current_utxo_sats": current_utxo_sats,
        "lifetime_received_sats": lifetime_received_sats,
        "lifetime_spent_sats": lifetime_spent_sats,
        "max_single_output_sats": 150,
        "max_same_tx_received_sats": 200,
        "inflow_30d_sats": 30,
        "outflow_30d_sats": 20,
        "gross_flow_30d_sats": 50,
        "inflow_90d_sats": 60,
        "outflow_90d_sats": 40,
        "gross_flow_90d_sats": 100,
        "gross_flow_365d_sats": 200,
        "direct_large_counterparty_count": 1,
    }
    values.update(updates)
    return AddressFeatureRow.model_validate(values)


def make_script(
    address: str | None = BTC_ADDRESSES[0],
    *,
    script_hex: str = "76a914" + "11" * 20 + "88ac",
    script_type: str = "p2pkh",
) -> ScriptSubjectRow:
    subject = normalize_bitcoin_address(address) if address else None
    return ScriptSubjectRow(
        script_id=hashlib.sha256(
            b"bitcoin:mainnet\x00" + bytes.fromhex(script_hex)
        ).hexdigest(),
        script_hex=script_hex,
        script_type=script_type,
        normalized_address=subject.normalized_address if subject else None,
        address_id=subject.address_id if subject else None,
        provider_enrichable=subject is not None,
    )


def make_accounting(**updates: int) -> UniverseCoverageCounters:
    values = {
        "total_output_rows": 10,
        "total_input_rows": 8,
        "distinct_script_subjects": 2,
        "standard_single_address_rows": 8,
        "empty_address_rows": 1,
        "multi_address_rows": 1,
        "nonstandard_rows": 1,
        "unmatched_input_rows": 0,
    }
    values.update(updates)
    return UniverseCoverageCounters.model_validate(values)
