"""Typed, secret-safe runtime configuration."""

from __future__ import annotations

import ipaddress
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for the BTC-first identity runtime."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore", case_sensitive=True)

    database_path: Path = Field(
        default=Path("data/identity/address_identity.sqlite3"),
        validation_alias="CAI_DATABASE_PATH",
    )
    raw_payload_root: Path = Field(
        default=Path("data/raw/0xrouter"), validation_alias="CAI_RAW_PAYLOAD_ROOT"
    )
    export_root: Path = Field(default=Path("data/exports"), validation_alias="CAI_EXPORT_ROOT")
    universe_root: Path = Field(
        default=Path("data/universe"), validation_alias="CAI_UNIVERSE_ROOT"
    )
    universe_duckdb_path: Path = Field(
        default=Path("data/universe/catalog.duckdb"),
        validation_alias="CAI_UNIVERSE_DUCKDB_PATH",
    )
    enabled_chains_value: str = Field(default="bitcoin", validation_alias="CAI_ENABLED_CHAINS")
    provider_base_url: str = Field(
        default="https://0xrouter.app", validation_alias="CAI_0XROUTER_BASE_URL"
    )
    provider_token: SecretStr | None = Field(default=None, validation_alias="CAI_0XROUTER_TOKEN")
    requests_per_minute: int = Field(
        default=20, ge=1, le=30, validation_alias="CAI_0XROUTER_REQUESTS_PER_MINUTE"
    )
    response_bytes_budget: int = Field(
        default=10_485_760, ge=1, validation_alias="CAI_0XROUTER_RESPONSE_BYTES_BUDGET"
    )
    http_timeout_seconds: int = Field(
        default=30, ge=1, validation_alias="CAI_HTTP_TIMEOUT_SECONDS"
    )
    max_transport_retries: int = Field(
        default=1, ge=0, le=3, validation_alias="CAI_0XROUTER_MAX_TRANSPORT_RETRIES"
    )
    discovery_ttl_hours: int = Field(
        default=168, ge=1, validation_alias="CAI_DISCOVERY_TTL_HOURS"
    )
    detail_ttl_hours: int = Field(default=720, ge=1, validation_alias="CAI_DETAIL_TTL_HOURS")
    max_detail_candidates_per_run: int = Field(
        default=100, ge=1, validation_alias="CAI_MAX_DETAIL_CANDIDATES_PER_RUN"
    )
    coverage_requests_per_minute: int = Field(
        default=25, ge=3, le=30, validation_alias="CAI_CHAINDATA_COVERAGE_REQUESTS_PER_MINUTE"
    )
    coverage_response_bytes_budget: int = Field(
        default=52_428_800,
        ge=1,
        validation_alias="CAI_CHAINDATA_COVERAGE_RESPONSE_BYTES_BUDGET",
    )
    coverage_address_ttl_hours: int = Field(
        default=336, ge=1, validation_alias="CAI_CHAINDATA_COVERAGE_ADDRESS_TTL_HOURS"
    )
    coverage_entity_ttl_hours: int = Field(
        default=720, ge=1, validation_alias="CAI_CHAINDATA_COVERAGE_ENTITY_TTL_HOURS"
    )
    coverage_prediction_retry_backoff_minutes: int = Field(
        default=60,
        ge=1,
        validation_alias="CAI_CHAINDATA_COVERAGE_PREDICTION_RETRY_BACKOFF_MINUTES",
    )
    coverage_max_entities_per_run: int = Field(
        default=8, ge=1, validation_alias="CAI_CHAINDATA_COVERAGE_MAX_ENTITIES_PER_RUN"
    )
    coverage_max_addresses_per_run: int = Field(
        default=100, ge=1, validation_alias="CAI_CHAINDATA_COVERAGE_MAX_ADDRESSES_PER_RUN"
    )
    bigquery_billing_project: str | None = Field(
        default=None, validation_alias="CAI_BIGQUERY_BILLING_PROJECT"
    )
    bigquery_dataset: str = Field(
        default="bigquery-public-data.crypto_bitcoin",
        validation_alias="CAI_BIGQUERY_DATASET",
    )
    bigquery_location: str = Field(
        default="US", validation_alias="CAI_BIGQUERY_LOCATION"
    )
    bigquery_maximum_bytes_billed: int = Field(
        default=0, ge=0, validation_alias="CAI_BIGQUERY_MAXIMUM_BYTES_BILLED"
    )
    bitcoin_rpc_url: str = Field(
        default="http://127.0.0.1:8332", validation_alias="CAI_BITCOIN_RPC_URL"
    )
    bitcoin_rpc_cookie_file: Path = Field(
        default=Path("~/.bitcoin/.cookie"),
        validation_alias="CAI_BITCOIN_RPC_COOKIE_FILE",
    )
    bitcoin_finality_depth: int = Field(
        default=6, ge=1, le=144, validation_alias="CAI_BITCOIN_FINALITY_DEPTH"
    )
    bitcoin_rpc_timeout_seconds: int = Field(
        default=30, ge=1, le=300, validation_alias="CAI_BITCOIN_RPC_TIMEOUT_SECONDS"
    )
    universe_max_source_age_hours: int = Field(
        default=48, ge=1, le=168, validation_alias="CAI_UNIVERSE_MAX_SOURCE_AGE_HOURS"
    )

    @field_validator("enabled_chains_value")
    @classmethod
    def validate_enabled_chains(cls, value: str) -> str:
        chains = tuple(item.strip().lower() for item in value.split(",") if item.strip())
        if chains != ("bitcoin",):
            raise ValueError("BTC-first phase requires CAI_ENABLED_CHAINS=bitcoin")
        return ",".join(chains)

    @field_validator("provider_base_url")
    @classmethod
    def validate_provider_base_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("CAI_0XROUTER_BASE_URL must be an HTTPS origin without userinfo")
        return value.rstrip("/")

    @field_validator("bitcoin_rpc_url")
    @classmethod
    def validate_bitcoin_rpc_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("CAI_BITCOIN_RPC_URL must be a credential-free HTTP(S) origin")
        try:
            is_loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            is_loopback = parsed.hostname.lower() == "localhost"
        if parsed.scheme == "http" and not is_loopback:
            raise ValueError("plain HTTP Bitcoin RPC is allowed only for loopback hosts")
        return value.rstrip("/")

    @model_validator(mode="after")
    def validate_distinct_runtime_paths(self) -> "Settings":
        paths = {
            self.database_path,
            self.raw_payload_root,
            self.export_root,
            self.universe_root,
            self.universe_duckdb_path,
        }
        if len(paths) != 5:
            raise ValueError("database, raw payload, export, and universe paths must be distinct")
        return self

    @property
    def enabled_chains(self) -> tuple[str, ...]:
        return tuple(self.enabled_chains_value.split(","))

    def provider_token_value(self) -> str | None:
        """Return a token only to the in-memory HTTP boundary."""

        return self.provider_token.get_secret_value() if self.provider_token else None

    def safe_summary(self) -> dict[str, object]:
        """Return configuration diagnostics without secret values."""

        return {
            "database_path": str(self.database_path),
            "raw_payload_root": str(self.raw_payload_root),
            "export_root": str(self.export_root),
            "universe_root": str(self.universe_root),
            "universe_duckdb_path": str(self.universe_duckdb_path),
            "enabled_chains": list(self.enabled_chains),
            "provider_base_url": self.provider_base_url,
            "provider_token_configured": self.provider_token is not None,
            "requests_per_minute": self.requests_per_minute,
            "response_bytes_budget": self.response_bytes_budget,
            "max_transport_retries": self.max_transport_retries,
            "coverage_requests_per_minute": self.coverage_requests_per_minute,
            "coverage_response_bytes_budget": self.coverage_response_bytes_budget,
            "coverage_address_ttl_hours": self.coverage_address_ttl_hours,
            "coverage_entity_ttl_hours": self.coverage_entity_ttl_hours,
            "coverage_prediction_retry_backoff_minutes": self.coverage_prediction_retry_backoff_minutes,
            "coverage_max_entities_per_run": self.coverage_max_entities_per_run,
            "coverage_max_addresses_per_run": self.coverage_max_addresses_per_run,
            "bigquery_billing_project_configured": self.bigquery_billing_project
            is not None,
            "bigquery_dataset": self.bigquery_dataset,
            "bigquery_location": self.bigquery_location,
            "bigquery_maximum_bytes_billed": self.bigquery_maximum_bytes_billed,
            "bitcoin_rpc_url": self.bitcoin_rpc_url,
            "bitcoin_finality_depth": self.bitcoin_finality_depth,
            "bitcoin_rpc_timeout_seconds": self.bitcoin_rpc_timeout_seconds,
            "universe_max_source_age_hours": self.universe_max_source_age_hours,
        }
