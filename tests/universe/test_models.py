from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.universe.models import (
    AddressFeatureRow,
    CampaignManifest,
    ScriptSubjectRow,
    SourceManifest,
    SourceProbeResult,
)


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
BTC_SUBJECT = normalize_bitcoin_address(BTC_ADDRESS)


def source_manifest() -> SourceManifest:
    return SourceManifest(
        campaign_id="btc-20260724",
        source_kind="bigquery",
        source_revision="schema-fixture",
        cutoff_height=900_000,
        cutoff_hash="01" * 32,
        cutoff_time=datetime(2026, 7, 24, tzinfo=UTC),
        schema_sha256="02" * 32,
        query_sha256="03" * 32,
        source_capabilities=("address_rows", "script_hex"),
        script_completeness=True,
    )


def address_feature(**updates: object) -> AddressFeatureRow:
    values: dict[str, object] = {
        "address_id": BTC_SUBJECT.address_id,
        "normalized_address": BTC_ADDRESS,
        "address_type": "p2pkh",
        "first_seen_height": 100,
        "last_seen_height": 200,
        "first_seen_time": datetime(2025, 1, 1, tzinfo=UTC),
        "last_seen_time": datetime(2026, 1, 1, tzinfo=UTC),
        "output_count": 3,
        "spent_output_count": 2,
        "transaction_count": 4,
        "current_utxo_sats": 100,
        "lifetime_received_sats": 300,
        "lifetime_spent_sats": 200,
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


def test_source_manifest_fingerprint_is_order_independent() -> None:
    first = source_manifest()
    second = first.model_copy(
        update={"source_capabilities": ("script_hex", "address_rows")}
    )

    assert first.manifest_sha256 == second.manifest_sha256


def test_source_manifest_fingerprint_changes_with_semantics() -> None:
    first = source_manifest()
    second = first.model_copy(update={"cutoff_height": first.cutoff_height + 1})

    assert first.manifest_sha256 != second.manifest_sha256


@pytest.mark.parametrize(
    "value",
    [
        {"cutoff_hash": "A1" * 32},
        {"schema_sha256": "0" * 63},
        {"query_sha256": "not-a-hash"},
        {"cutoff_time": datetime(2026, 7, 24)},
    ],
)
def test_source_manifest_rejects_invalid_hashes_or_naive_time(
    value: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        SourceManifest.model_validate({**source_manifest().model_dump(), **value})


def test_source_probe_normalizes_aware_datetimes_to_utc() -> None:
    result = SourceProbeResult(
        source_kind="bitcoin_core",
        status="accepted",
        schema_sha256=None,
        latest_height=900_010,
        latest_hash="10" * 32,
        latest_time=datetime(2026, 7, 24, tzinfo=UTC),
        finalized_height=900_004,
        finalized_hash="11" * 32,
        dry_run_bytes=None,
        script_completeness=True,
        capabilities=("historical_block_scan",),
    )

    assert result.latest_time == datetime(2026, 7, 24, tzinfo=UTC)
    assert result.read_only is True


def test_address_feature_rejects_negative_or_ambiguous_values() -> None:
    with pytest.raises(ValidationError):
        address_feature(current_utxo_sats=-1)

    with pytest.raises(ValidationError):
        address_feature(normalized_address="not-a-bitcoin-address")


def test_script_subject_recomputes_script_id_and_preserves_non_address_script() -> None:
    script_hex = "76a914" + "11" * 20 + "88ac"
    script_id = hashlib.sha256(
        b"bitcoin:mainnet\x00" + bytes.fromhex(script_hex)
    ).hexdigest()

    row = ScriptSubjectRow(
        script_id=script_id,
        script_hex=script_hex,
        script_type="p2pkh",
        normalized_address=BTC_ADDRESS,
        address_id=BTC_SUBJECT.address_id,
        provider_enrichable=True,
    )
    non_address = ScriptSubjectRow(
        script_id=hashlib.sha256(
            b"bitcoin:mainnet\x00" + bytes.fromhex("6a01ff")
        ).hexdigest(),
        script_hex="6a01ff",
        script_type="op_return",
        normalized_address=None,
        address_id=None,
        provider_enrichable=False,
    )

    assert row.address_id == BTC_SUBJECT.address_id
    assert non_address.normalized_address is None


@pytest.mark.parametrize(
    "updates",
    [
        {"script_id": "00" * 32},
        {"normalized_address": BTC_ADDRESS, "address_id": None},
        {
            "normalized_address": None,
            "address_id": BTC_SUBJECT.address_id,
            "provider_enrichable": False,
        },
        {
            "normalized_address": None,
            "address_id": None,
            "provider_enrichable": True,
        },
    ],
)
def test_script_subject_rejects_inconsistent_identity_mapping(
    updates: dict[str, object],
) -> None:
    script_hex = "76a914" + "11" * 20 + "88ac"
    values: dict[str, object] = {
        "script_id": hashlib.sha256(
            b"bitcoin:mainnet\x00" + bytes.fromhex(script_hex)
        ).hexdigest(),
        "script_hex": script_hex,
        "script_type": "p2pkh",
        "normalized_address": BTC_ADDRESS,
        "address_id": BTC_SUBJECT.address_id,
        "provider_enrichable": True,
    }
    values.update(updates)

    with pytest.raises(ValidationError):
        ScriptSubjectRow.model_validate(values)


def test_campaign_manifest_cannot_claim_output_fact_materialization() -> None:
    manifest = CampaignManifest(
        campaign_id="btc-20260724",
        source_manifest=source_manifest(),
        created_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    assert manifest.output_fact_materialized is False
    with pytest.raises(ValidationError):
        CampaignManifest(
            campaign_id="btc-20260724",
            source_manifest=source_manifest(),
            created_at=datetime(2026, 7, 24, tzinfo=UTC),
            output_fact_materialized=True,
        )
