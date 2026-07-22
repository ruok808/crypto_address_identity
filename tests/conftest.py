from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def runtime_root(tmp_path: Path) -> Path:
    root = tmp_path / "runtime"
    root.mkdir()
    return root


@pytest.fixture
def env_mapping(runtime_root: Path) -> dict[str, str]:
    return {
        "CAI_DATABASE_PATH": str(runtime_root / "identity.sqlite3"),
        "CAI_RAW_PAYLOAD_ROOT": str(runtime_root / "raw"),
        "CAI_EXPORT_ROOT": str(runtime_root / "exports"),
        "CAI_ENABLED_CHAINS": "bitcoin",
        "CAI_0XROUTER_BASE_URL": "https://0xrouter.test",
        "CAI_0XROUTER_REQUESTS_PER_MINUTE": "20",
        "CAI_0XROUTER_RESPONSE_BYTES_BUDGET": "1048576",
        "CAI_HTTP_TIMEOUT_SECONDS": "5",
        "CAI_DISCOVERY_TTL_HOURS": "168",
        "CAI_DETAIL_TTL_HOURS": "720",
        "CAI_MAX_DETAIL_CANDIDATES_PER_RUN": "5",
    }
