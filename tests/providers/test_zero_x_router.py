from __future__ import annotations

import json

import httpx
import pytest

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.core.config import Settings
from crypto_address_identity.providers.zero_x_router import (
    ProviderPayloadError,
    ProviderProfile,
    ProviderTokenMissing,
    ZeroXRouterClient,
    parse_bitcoin_response,
)


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def _settings(env_mapping: dict[str, str], *, token: str | None = None) -> Settings:
    if token is not None:
        env_mapping["CAI_0XROUTER_TOKEN"] = token
    return Settings.model_validate(env_mapping)


def _populated_payload() -> bytes:
    return json.dumps(
        {
            "bitcoin": {
                "address": BTC_ADDRESS,
                "chain": "bitcoin",
                "isUserAddress": True,
                "arkhamEntity": {"id": "entity-1", "name": "Example Exchange", "type": "exchange"},
                "arkhamLabel": {"address": BTC_ADDRESS, "chainType": "bitcoin", "name": "Example wallet"},
                "populatedTags": [
                    {"id": "tag-1", "label": "exchange", "chain": "bitcoin", "rank": 1},
                    {"id": "tag-2", "label": "hot", "chain": "bitcoin", "rank": 2},
                ],
            }
        }
    ).encode()


def test_request_profiles_use_expected_query_parameters_and_no_token_in_url(
    env_mapping: dict[str, str]
) -> None:
    settings = _settings(env_mapping, token="test-token")
    client = ZeroXRouterClient(settings, transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})))

    discovery = client.build_request(BTC_ADDRESS, ProviderProfile.DISCOVERY)
    detail = client.build_request(BTC_ADDRESS, ProviderProfile.DETAIL)

    assert discovery.url.params["includeTags"] == "false"
    assert discovery.url.params["includeEntityPredictions"] == "true"
    assert discovery.url.params["includeClusters"] == "false"
    assert detail.url.params["includeTags"] == "true"
    assert "test-token" not in str(discovery.url)
    assert discovery.headers["X-My-Token"] == "test-token"


def test_execute_request_requires_token_without_exposing_it(env_mapping: dict[str, str]) -> None:
    client = ZeroXRouterClient(_settings(env_mapping), transport=httpx.MockTransport(lambda request: httpx.Response(200)))

    with pytest.raises(ProviderTokenMissing):
        client.build_request(BTC_ADDRESS, ProviderProfile.DISCOVERY)


def test_parser_creates_tier_c_candidates_and_no_wallet_role_claim() -> None:
    parsed = parse_bitcoin_response(_populated_payload(), normalize_bitcoin_address(BTC_ADDRESS))

    assert parsed.extension_metadata["isUserAddress"] is True
    assert len(parsed.evidence_candidates) == 4
    assert {candidate.assertion_type for candidate in parsed.evidence_candidates} == {
        "entity_control",
        "address_label",
    }
    assert all(candidate.evidence_tier == "C" for candidate in parsed.evidence_candidates)
    assert all(candidate.candidate_wallet_role is None for candidate in parsed.evidence_candidates)
    assert len(parsed.schema_fingerprint) == 64


def test_parser_accepts_missing_optional_labels_without_negative_evidence() -> None:
    payload = json.dumps(
        {"bitcoin": {"address": BTC_ADDRESS, "chain": "bitcoin", "isUserAddress": False}}
    ).encode()

    parsed = parse_bitcoin_response(payload, normalize_bitcoin_address(BTC_ADDRESS))

    assert parsed.evidence_candidates == ()
    assert parsed.extension_metadata["isUserAddress"] is False


@pytest.mark.parametrize(
    "payload",
    [
        {"bitcoin": {"address": BTC_ADDRESS, "chain": "ethereum"}},
        {"bitcoin": {"address": "1CounterpartyXX", "chain": "bitcoin"}},
        {"bitcoin": {"address": BTC_ADDRESS, "chain": "bitcoin", "arkhamEntity": "bad"}},
        {"bitcoin": {"address": BTC_ADDRESS, "chain": "bitcoin", "populatedTags": {}}},
    ],
)
def test_parser_rejects_malformed_or_mismatched_root(payload: dict[str, object]) -> None:
    with pytest.raises(ProviderPayloadError):
        parse_bitcoin_response(json.dumps(payload).encode(), normalize_bitcoin_address(BTC_ADDRESS))


def test_429_is_returned_once_without_hidden_retry(env_mapping: dict[str, str]) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, content=b"rate limited")

    client = ZeroXRouterClient(_settings(env_mapping, token="test-token"), transport=httpx.MockTransport(handler))
    response = client.fetch(BTC_ADDRESS, ProviderProfile.DISCOVERY)

    assert response.http_status == 429
    assert response.outcome == "rate_limited"
    assert response.body == b"rate limited"
    assert calls == 1


def test_non_success_status_is_returned_without_transport_retry(env_mapping: dict[str, str]) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, content=b"fixture unavailable")

    client = ZeroXRouterClient(
        _settings(env_mapping, token="test-token"), transport=httpx.MockTransport(handler)
    )

    response = client.fetch(BTC_ADDRESS, ProviderProfile.DISCOVERY)

    assert response.http_status == 503
    assert response.outcome == "http_error"
    assert calls == 1


def test_transport_error_retries_within_configured_bound(env_mapping: dict[str, str]) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("fixture transport failure", request=request)
        return httpx.Response(200, content=_populated_payload())

    client = ZeroXRouterClient(
        _settings(env_mapping, token="test-token"), transport=httpx.MockTransport(handler)
    )

    response = client.fetch(BTC_ADDRESS, ProviderProfile.DISCOVERY)

    assert response.outcome == "success"
    assert calls == 2


def test_timeout_exhaustion_is_reported_as_transport_error(env_mapping: dict[str, str]) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("fixture timeout", request=request)

    client = ZeroXRouterClient(
        _settings(env_mapping, token="test-token"), transport=httpx.MockTransport(handler)
    )

    response = client.fetch(BTC_ADDRESS, ProviderProfile.DISCOVERY)

    assert response.http_status is None
    assert response.outcome == "transport_error"
    assert response.body == b""
    assert calls == 2


def test_coverage_requests_stay_on_btc_routes_and_disable_unneeded_expansions(
    env_mapping: dict[str, str]
) -> None:
    client = ZeroXRouterClient(
        _settings(env_mapping, token="test-token"),
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    )

    enrichment = client.build_btc_coverage_enrichment_request(BTC_ADDRESS)
    predictions = client.build_entity_predictions_request("binance")
    ranking = client.build_entity_balance_changes_request(
        entity_types=("exchange", "fund"), interval="30d", order_by="balanceUsdChange"
    )

    assert enrichment.url.path.endswith(f"/address_enriched/{BTC_ADDRESS}")
    assert not enrichment.url.path.endswith("/all")
    assert enrichment.url.params["includeTags"] == "true"
    assert enrichment.url.params["includeEntityPredictions"] == "false"
    assert enrichment.url.params["includeClusters"] == "false"
    assert predictions.url.path.endswith("/entity_predictions/binance")
    assert ranking.url.params["chains"] == "bitcoin"
    assert ranking.url.params["entityTypes"] == "exchange,fund"
    assert ranking.url.params["limit"] == "100"
    assert "test-token" not in str(enrichment.url)
