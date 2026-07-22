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
