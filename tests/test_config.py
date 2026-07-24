from __future__ import annotations

import pytest
from pydantic import ValidationError

from crypto_address_identity.core.config import Settings


def test_settings_parse_non_secret_runtime_contract(env_mapping: dict[str, str]) -> None:
    settings = Settings.model_validate(env_mapping)

    assert settings.enabled_chains == ("bitcoin",)
    assert settings.requests_per_minute == 20
    assert settings.provider_token is None
    assert settings.provider_base_url == "https://0xrouter.test"


def test_universe_settings_are_non_secret_and_fail_closed(
    env_mapping: dict[str, str],
) -> None:
    settings = Settings.model_validate(env_mapping)

    assert settings.bigquery_maximum_bytes_billed == 0
    assert settings.bitcoin_finality_depth == 6
    assert settings.safe_summary()["bigquery_billing_project_configured"] is True
    assert "credentials" not in settings.safe_summary()
    assert "bitcoin_rpc_cookie" not in settings.safe_summary()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("CAI_0XROUTER_REQUESTS_PER_MINUTE", "0"),
        ("CAI_0XROUTER_REQUESTS_PER_MINUTE", "31"),
        ("CAI_0XROUTER_RESPONSE_BYTES_BUDGET", "0"),
        ("CAI_ENABLED_CHAINS", "bitcoin,ethereum"),
        ("CAI_0XROUTER_BASE_URL", "http://example.test"),
    ],
)
def test_settings_reject_invalid_or_non_btc_phase_values(
    env_mapping: dict[str, str], field: str, value: str
) -> None:
    env_mapping[field] = value

    with pytest.raises(ValidationError):
        Settings.model_validate(env_mapping)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("CAI_BIGQUERY_MAXIMUM_BYTES_BILLED", "-1"),
        ("CAI_BITCOIN_FINALITY_DEPTH", "0"),
        ("CAI_BITCOIN_RPC_URL", "http://remote.example:8332"),
        ("CAI_BITCOIN_RPC_URL", "https://user:password@example.test"),
    ],
)
def test_universe_settings_reject_unsafe_values(
    env_mapping: dict[str, str], field: str, value: str
) -> None:
    env_mapping[field] = value
    with pytest.raises(ValidationError):
        Settings.model_validate(env_mapping)


@pytest.mark.parametrize(
    ("field", "other_field"),
    [
        ("CAI_UNIVERSE_ROOT", "CAI_DATABASE_PATH"),
        ("CAI_UNIVERSE_ROOT", "CAI_RAW_PAYLOAD_ROOT"),
        ("CAI_UNIVERSE_DUCKDB_PATH", "CAI_EXPORT_ROOT"),
    ],
)
def test_universe_paths_must_be_distinct_from_existing_runtime_paths(
    env_mapping: dict[str, str], field: str, other_field: str
) -> None:
    env_mapping[field] = env_mapping[other_field]
    with pytest.raises(ValidationError):
        Settings.model_validate(env_mapping)
