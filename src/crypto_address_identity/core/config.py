"""Typed, secret-safe runtime configuration."""

from __future__ import annotations

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
    coverage_max_entities_per_run: int = Field(
        default=8, ge=1, validation_alias="CAI_CHAINDATA_COVERAGE_MAX_ENTITIES_PER_RUN"
    )
    coverage_max_addresses_per_run: int = Field(
        default=100, ge=1, validation_alias="CAI_CHAINDATA_COVERAGE_MAX_ADDRESSES_PER_RUN"
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

    @model_validator(mode="after")
    def validate_distinct_runtime_paths(self) -> "Settings":
        paths = {self.database_path, self.raw_payload_root, self.export_root}
        if len(paths) != 3:
            raise ValueError("database, raw payload, and export paths must be distinct")
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
            "coverage_max_entities_per_run": self.coverage_max_entities_per_run,
            "coverage_max_addresses_per_run": self.coverage_max_addresses_per_run,
        }
