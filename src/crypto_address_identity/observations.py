"""Safe source-observation metadata without request credentials."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


class SecretBoundaryError(ValueError):
    """Raised when proposed observation metadata could contain a secret."""


@dataclass(frozen=True)
class ObservationMetadata:
    endpoint_template: str
    query_profile: str


def build_observation_metadata(*, endpoint_template: str, query_profile: str) -> ObservationMetadata:
    """Validate a route class suitable for audit storage.

    Credentials, query values, and fragments are intentionally excluded because
    observations are durable audit records.
    """

    if not endpoint_template or "?" in endpoint_template or "#" in endpoint_template:
        raise SecretBoundaryError("Observation endpoint template must not include query or fragment")
    lowered = endpoint_template.lower()
    prohibited_markers = ("token", "apikey", "api_key", "authorization", "bearer")
    if any(marker in lowered for marker in prohibited_markers):
        raise SecretBoundaryError("Observation endpoint template contains a prohibited secret marker")

    parsed = urlparse(endpoint_template)
    if parsed.scheme:
        if parsed.scheme != "https" or parsed.username or parsed.password:
            raise SecretBoundaryError("Observation endpoint template must not include credentials")
    elif not endpoint_template.startswith("/"):
        raise ValueError("Observation endpoint template must be an HTTPS URL or absolute route")
    if query_profile not in {"discovery", "detail", "import"}:
        raise ValueError("Unsupported observation query profile")
    return ObservationMetadata(endpoint_template=endpoint_template, query_profile=query_profile)
