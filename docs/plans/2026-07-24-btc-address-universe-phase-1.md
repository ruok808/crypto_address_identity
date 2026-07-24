# BTC Address Universe Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the first, provider-free phase of `docs/designs/2026-07-24-btc-address-universe-bootstrap-design.md`: read-only BigQuery and Bitcoin Core source probes, a checksum-pinned BTC chain-universe store, and deterministic dry-run statistics for the first enrichment wave.

**Architecture:** Keep large chain facts and address features outside the identity SQLite database in partitioned Parquet queried through DuckDB. Put BigQuery and Bitcoin Core behind source-specific adapters that emit one source-neutral manifest contract. Phase 1 may read public chain data under an explicit BigQuery byte cap, but it must never instantiate the 0xRouter client, spend provider points, append Tier C evidence, populate the existing provider candidate queue, or start a canary.

**Tech Stack:** Python 3.13, Pydantic, httpx, DuckDB, PyArrow/Parquet, optional `google-cloud-bigquery`, pytest, SHA-256, Bitcoin Core JSON-RPC.

---

## Scope And Stop Boundary

Phase 1 ends after producing these artifacts:

1. A BigQuery source-probe result containing schema, partitioning, freshness,
   proposed cutoff, dry-run bytes, and query hashes.
2. A Bitcoin Core source-probe result containing mainnet/sync/pruning status,
   finalized checkpoint height/hash, and source capabilities.
3. A checksum-pinned local universe campaign containing source manifests,
   distinct output-script subjects, address features, source-accounting
   counters, and no identity evidence.
4. Aggregate-only first-wave dry-run statistics with exact cohort overlap and
   deduplication counts.

Phase 1 does not:

- call any `/chaindata` or Arkham/0xRouter route;
- read `CAI_0XROUTER_TOKEN`;
- write `address_subject`, `candidate_request`, `source_observation`,
  `identity_evidence`, claims, conflicts, resolutions, or resolver snapshots;
- activate, modify, or run the existing coverage-sync LaunchAgent;
- infer an entity, wallet owner, cluster, change address, or common-input owner;
- approve or execute the 1,000-address provider canary; or
- change any `quant_crypto` consumer.

The words `dry-run` have two separate, explicit meanings:

- **BigQuery dry run:** compile the exact chain query and return bytes processed
  without executing the data query.
- **Candidate statistics dry run:** read an already accepted local universe
  snapshot and return aggregate selection statistics without writing candidate
  rows or address values anywhere.

An optional real BigQuery chain read is needed to obtain real address counts.
It is still Phase 1 because it reads public chain data only, but it requires a
separate `--execute-chain-read` flag, an operator-supplied
`--maximum-bytes-billed`, and review of the preceding BigQuery dry-run output.

## File Structure

Create:

- `src/crypto_address_identity/universe/__init__.py` - public Phase 1 exports.
- `src/crypto_address_identity/universe/models.py` - source manifests, probe
  results, script subjects, address features, coverage counters, and
  statistics contracts.
- `src/crypto_address_identity/universe/bitcoin_core.py` - read-only JSON-RPC
  client and source probe.
- `src/crypto_address_identity/universe/bigquery.py` - optional SDK boundary,
  schema/freshness probe, dry-run estimator, and bounded row streaming.
- `src/crypto_address_identity/universe/query_plan.py` - immutable BigQuery
  query loading, parameter contracts, and query hashing.
- `src/crypto_address_identity/universe/anchors.py` - read-only extraction of
  exact Tier A/B and active-conflict address ids into a checksum-pinned
  calibration snapshot.
- `src/crypto_address_identity/universe/storage.py` - atomic campaign
  directory, Parquet partitions, checksums, and read-only DuckDB views.
- `src/crypto_address_identity/universe/features.py` - source-row validation
  and address-feature materialization.
- `src/crypto_address_identity/universe/policy.py` - versioned P0/P1 policy and
  deterministic cohort ordering.
- `src/crypto_address_identity/universe/statistics.py` - aggregate-only dry-run
  selection report.
- `src/crypto_address_identity/universe/sql/bigquery/address_features.sql` -
  one address-level full-history query with partition-aware rolling windows.
- `src/crypto_address_identity/universe/sql/bigquery/source_checkpoint.sql` -
  bounded recent-partition checkpoint and Taproot capability probe.
- `src/crypto_address_identity/universe/sql/duckdb/schema.sql` - source-neutral
  universe tables/views.
- `tests/universe/fixtures/bigquery_schema.json` - sanitized table metadata.
- `tests/universe/fixtures/bitcoin_core_responses.json` - sanitized RPC
  responses.
- `tests/universe/conftest.py` - explicit Arrow/script/address fixture builders.
- `tests/universe/test_models.py`
- `tests/universe/test_bitcoin_core_probe.py`
- `tests/universe/test_bigquery_probe.py`
- `tests/universe/test_storage.py`
- `tests/universe/test_features.py`
- `tests/universe/test_anchors.py`
- `tests/universe/test_policy.py`
- `tests/universe/test_statistics.py`
- `tests/universe/test_cli.py`

Modify:

- `pyproject.toml` - DuckDB/PyArrow runtime dependencies and optional BigQuery
  extra.
- `src/crypto_address_identity/core/config.py` - non-secret universe/source
  settings.
- `src/crypto_address_identity/cli.py` - `universe` command group.
- `conf/env/address_identity.env.example` - non-secret Phase 1 examples.
- `README.md` - current universe boundary and commands.
- `docs/btc_identity_operations.md` - source probe, build, and approval gates.
- `tests/conftest.py` - temporary universe-root configuration.
- `tests/test_config.py` - new settings and URL/path validation.

Generate Parquet fixtures from explicit Python rows inside `tests/universe`
fixtures; do not commit opaque binary fixture files.

Do not modify:

- `src/crypto_address_identity/coverage.py`
- `src/crypto_address_identity/fetch.py`
- `src/crypto_address_identity/providers/zero_x_router.py`
- `src/crypto_address_identity/evidence.py`
- existing SQLite migrations
- `ops/launchd/`

## Task 1: Add Phase 1 Dependencies And Typed Settings

**Files:**

- Modify: `pyproject.toml`
- Modify: `src/crypto_address_identity/core/config.py`
- Modify: `conf/env/address_identity.env.example`
- Modify: `tests/conftest.py`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write failing settings tests**

Add tests that require distinct universe paths, a loopback-only HTTP Bitcoin
RPC URL, a non-negative BigQuery byte budget, and no BigQuery credential value
in `safe_summary()`:

```python
def test_universe_settings_are_non_secret_and_fail_closed(
    env_mapping: dict[str, str],
) -> None:
    env_mapping.update(
        {
            "CAI_UNIVERSE_ROOT": "/tmp/cai-universe",
            "CAI_UNIVERSE_DUCKDB_PATH": "/tmp/cai-universe/catalog.duckdb",
            "CAI_BIGQUERY_BILLING_PROJECT": "fixture-project",
            "CAI_BIGQUERY_MAXIMUM_BYTES_BILLED": "0",
            "CAI_BITCOIN_RPC_URL": "http://127.0.0.1:8332",
            "CAI_BITCOIN_FINALITY_DEPTH": "6",
        }
    )
    settings = Settings.model_validate(env_mapping)

    assert settings.bigquery_maximum_bytes_billed == 0
    assert settings.bitcoin_finality_depth == 6
    assert settings.safe_summary()["bigquery_billing_project_configured"] is True
    assert "credentials" not in settings.safe_summary()
    assert "bitcoin_rpc_cookie" not in settings.safe_summary()


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
```

- [ ] **Step 2: Run the settings tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_config.py
```

Expected: failures because the `CAI_UNIVERSE_*`, `CAI_BIGQUERY_*`, and
`CAI_BITCOIN_RPC_*` fields do not exist.

- [ ] **Step 3: Add dependencies and settings**

Add `duckdb>=1.3,<2` and `pyarrow>=20,<24` to project dependencies. Add an
optional extra:

```toml
bigquery = [
  "google-cloud-bigquery>=3.35,<4",
  "google-cloud-bigquery-storage>=2.32,<3",
]
```

Add these settings:

```python
universe_root: Path = Field(
    default=Path("data/universe"), validation_alias="CAI_UNIVERSE_ROOT"
)
universe_duckdb_path: Path = Field(
    default=Path("data/universe/catalog.duckdb"),
    validation_alias="CAI_UNIVERSE_DUCKDB_PATH",
)
bigquery_billing_project: str | None = Field(
    default=None, validation_alias="CAI_BIGQUERY_BILLING_PROJECT"
)
bigquery_dataset: str = Field(
    default="bigquery-public-data.crypto_bitcoin",
    validation_alias="CAI_BIGQUERY_DATASET",
)
bigquery_location: str = Field(default="US", validation_alias="CAI_BIGQUERY_LOCATION")
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
```

Allow plain HTTP only for loopback RPC hosts. Require HTTPS and reject URL
userinfo for every non-loopback host. Expand and resolve the cookie path only at
the RPC boundary; do not print it in `safe_summary()`.

- [ ] **Step 4: Run settings tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_config.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 1**

```bash
git add pyproject.toml src/crypto_address_identity/core/config.py \
  conf/env/address_identity.env.example tests/conftest.py tests/test_config.py
git commit -m "feat: add BTC universe source settings"
```

## Task 2: Define Source-Neutral Universe Contracts

**Files:**

- Create: `src/crypto_address_identity/universe/__init__.py`
- Create: `src/crypto_address_identity/universe/models.py`
- Create: `tests/universe/test_models.py`

- [ ] **Step 1: Write failing contract tests**

Cover timezone enforcement, SHA-256 lengths, non-negative satoshis, one-address
mapping, and manifest determinism:

```python
def test_source_manifest_fingerprint_is_order_independent() -> None:
    first = SourceManifest(
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
    second = first.model_copy(
        update={"source_capabilities": ("script_hex", "address_rows")}
    )

    assert first.manifest_sha256 == second.manifest_sha256


def test_address_feature_rejects_negative_or_ambiguous_values() -> None:
    with pytest.raises(ValidationError):
        AddressFeatureRow(
            address_id="04" * 32,
            normalized_address=BTC_ADDRESS,
            address_type="p2pkh",
            current_utxo_sats=-1,
            lifetime_received_sats=0,
            lifetime_spent_sats=0,
            max_single_output_sats=0,
            max_same_tx_received_sats=0,
            gross_flow_30d_sats=0,
            gross_flow_90d_sats=0,
            gross_flow_365d_sats=0,
            last_seen_height=900_000,
        )
```

- [ ] **Step 2: Run the model tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_models.py
```

Expected: import failures for the missing `universe.models` module.

- [ ] **Step 3: Implement the contracts**

Implement these stable model names:

```python
class SourceProbeResult(BaseModel):
    source_kind: Literal["bigquery", "bitcoin_core"]
    status: Literal["accepted", "blocked", "partial"]
    read_only: bool = True
    schema_sha256: str | None
    latest_height: int | None
    latest_hash: str | None
    latest_time: datetime | None
    finalized_height: int | None
    finalized_hash: str | None
    dry_run_bytes: int | None
    script_completeness: bool
    capabilities: tuple[str, ...]
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


class SourceManifest(BaseModel):
    campaign_id: str
    source_kind: Literal["bigquery", "bitcoin_core", "fixture"]
    source_revision: str
    cutoff_height: int
    cutoff_hash: str
    cutoff_time: datetime
    schema_sha256: str
    query_sha256: str | None
    source_capabilities: tuple[str, ...]
    script_completeness: bool

    @computed_field
    @property
    def manifest_sha256(self) -> str:
        payload = self.model_dump(mode="json", exclude={"manifest_sha256"})
        payload["source_capabilities"] = sorted(payload["source_capabilities"])
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()


class AddressFeatureRow(BaseModel):
    feature_version: Literal["btc_address_features_v1"] = "btc_address_features_v1"
    address_id: str
    normalized_address: str
    address_type: str
    first_seen_height: int
    last_seen_height: int
    first_seen_time: datetime
    last_seen_time: datetime
    output_count: int
    spent_output_count: int
    transaction_count: int
    current_utxo_sats: int
    lifetime_received_sats: int
    lifetime_spent_sats: int
    max_single_output_sats: int
    max_same_tx_received_sats: int
    inflow_30d_sats: int
    outflow_30d_sats: int
    gross_flow_30d_sats: int
    inflow_90d_sats: int
    outflow_90d_sats: int
    gross_flow_90d_sats: int
    gross_flow_365d_sats: int
    direct_large_counterparty_count: int
```

Also define `UniverseCoverageCounters`, `CandidateReasonCount`,
`CandidateStatistics`, `CampaignManifest`, and:

```python
class ScriptSubjectRow(BaseModel):
    script_id: str
    script_hex: str
    script_type: str
    normalized_address: str | None
    address_id: str | None
    provider_enrichable: bool
```

`CampaignManifest` must include
`output_fact_materialized: Literal[False] = False`. Phase 1 screens and stores
distinct scripts and address aggregates; it does not claim that the later
Bitcoin Core per-output fact table has been materialized.

Compute:

```python
script_id = hashlib.sha256(
    b"bitcoin:mainnet\x00" + bytes.fromhex(script_hex)
).hexdigest()
```

Exactly zero or one normalized address may map to a script. Multi-address,
empty-address, P2PK, bare multisig, `OP_RETURN`, and unknown scripts retain the
script row but have null address fields and `provider_enrichable=false`. Every
datetime must be UTC-aware; every count and satoshi field must be non-negative;
hashes must be lowercase 64-character hexadecimal.

- [ ] **Step 4: Run model tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_models.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/crypto_address_identity/universe tests/universe/test_models.py
git commit -m "feat: define BTC universe contracts"
```

## Task 3: Implement The Read-Only Bitcoin Core Probe

**Files:**

- Create: `src/crypto_address_identity/universe/bitcoin_core.py`
- Create: `tests/universe/fixtures/bitcoin_core_responses.json`
- Create: `tests/universe/test_bitcoin_core_probe.py`

- [ ] **Step 1: Write failing probe tests**

Use `httpx.MockTransport` and a temporary cookie file. Assert the exact RPC
method sequence:

```python
def test_bitcoin_core_probe_accepts_synced_archival_mainnet(
    tmp_path: Path,
) -> None:
    transport, calls = rpc_fixture_transport(
        blockchain_info={
            "chain": "main",
            "blocks": 900_010,
            "headers": 900_010,
            "bestblockhash": "10" * 32,
            "initialblockdownload": False,
            "verificationprogress": 1.0,
            "pruned": False,
        },
        finalized_hash="11" * 32,
        finalized_header={"height": 900_004, "time": 1784851200, "confirmations": 7},
    )
    result = BitcoinCoreProbe(settings(tmp_path), transport=transport).run()

    assert [call["method"] for call in calls] == [
        "getblockchaininfo",
        "getblockhash",
        "getblockheader",
        "getindexinfo",
    ]
    assert result.status == "accepted"
    assert result.finalized_height == 900_004
    assert result.finalized_hash == "11" * 32
    assert result.script_completeness is True
```

Add blocked cases for testnet, initial block download, headers lag, shallow
finality, malformed result, mismatched header height, and remote HTTP URL.
Pruned nodes return `partial` with `utxo_probe` capability but without
`historical_block_scan`.

- [ ] **Step 2: Run the probe tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_bitcoin_core_probe.py
```

Expected: missing module/import failures.

- [ ] **Step 3: Implement the RPC boundary**

Implement a client with:

```python
class BitcoinCoreRpc:
    def __init__(
        self,
        *,
        url: str,
        cookie_file: Path,
        timeout_seconds: int,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        username, password = _read_cookie(cookie_file)
        self._client = httpx.Client(
            base_url=url,
            auth=(username, password),
            timeout=timeout_seconds,
            transport=transport,
            trust_env=False,
        )

    def call(self, method: str, params: list[object] | None = None) -> object:
        request_id = next(self._request_ids)
        response = self._client.post(
            "",
            json={
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params or [],
            },
        )
        response.raise_for_status()
        decoded = response.json()
        if decoded.get("id") != request_id or decoded.get("error") is not None:
            raise BitcoinCoreRpcError("Bitcoin Core returned a rejected RPC result")
        return decoded["result"]
```

`BitcoinCoreProbe.run()` must call only `getblockchaininfo`,
`getblockhash(finalized_height)`, `getblockheader(finalized_hash, true)`, and
`getindexinfo`. It must not call `dumptxoutset`, `getblock`, wallet methods, or
any write RPC. Never include cookie content, authorization headers, or raw RPC
errors in output.

- [ ] **Step 4: Run probe tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_bitcoin_core_probe.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 3**

```bash
git add src/crypto_address_identity/universe/bitcoin_core.py \
  tests/universe/fixtures/bitcoin_core_responses.json \
  tests/universe/test_bitcoin_core_probe.py
git commit -m "feat: add read-only Bitcoin Core source probe"
```

## Task 4: Implement BigQuery Schema Probe And Dry-Run Planner

**Files:**

- Create: `src/crypto_address_identity/universe/query_plan.py`
- Create: `src/crypto_address_identity/universe/bigquery.py`
- Create: `src/crypto_address_identity/universe/sql/bigquery/address_features.sql`
- Create: `src/crypto_address_identity/universe/sql/bigquery/source_checkpoint.sql`
- Create: `tests/universe/fixtures/bigquery_schema.json`
- Create: `tests/universe/test_bigquery_probe.py`

- [ ] **Step 1: Write failing BigQuery boundary tests**

Define a fake backend so tests never need credentials or network:

```python
class FakeBigQueryBackend:
    def __init__(self, *, tables: dict[str, TableMetadata], dry_run_bytes: int):
        self.tables = tables
        self.dry_run_bytes = dry_run_bytes
        self.executed_queries: list[str] = []

    def table_metadata(self, table_id: str) -> TableMetadata:
        return self.tables[table_id]

    def dry_run(
        self, sql: str, parameters: dict[str, object], maximum_bytes_billed: int
    ) -> QueryEstimate:
        self.executed_queries.append(sql)
        return QueryEstimate(total_bytes_processed=self.dry_run_bytes, cache_hit=False)
```

Test that accepted metadata requires:

```text
outputs:
  block_number, block_hash, block_timestamp, transaction_hash,
  transaction_index, index,
  script_hex, type, addresses, value
inputs:
  block_number, block_hash, block_timestamp, transaction_hash,
  transaction_index, index,
  spent_transaction_hash, spent_output_index, type, addresses, value
```

Assert `value` is an integer satoshi field, `addresses` is repeated string,
`script_hex` is retained, tables are time-partitioned, the query includes
`ARRAY_LENGTH(addresses) = 1`, and the query hash changes when its SQL changes.
The bounded checkpoint query must scan only the configured recent UTC
partitions and return a latest height/hash/time plus at least one valid recent
`bc1p` address or an explicit zero-count Taproot warning. Missing
`script_hex`, stale metadata, or a dry-run estimate above the supplied cap must
block acceptance.

- [ ] **Step 2: Run the BigQuery tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_bigquery_probe.py
```

Expected: missing `bigquery` and `query_plan` modules.

- [ ] **Step 3: Implement the query plan**

Load SQL with `importlib.resources`; never construct table identifiers from raw
user input. Validate the configured dataset against:

```python
_DATASET_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{4,62}\.[A-Za-z_][A-Za-z0-9_]*$")
```

The query must scan `inputs` and `outputs` once each, preserve source-accounting
counters for empty/multi-address/nonstandard rows, and emit one row per
distinct output script plus one row per canonical source address. Its final
projection uses `row_kind`:

```text
script_subject   -> columns matching ScriptSubjectRow
address_feature  -> columns matching AddressFeatureRow
source_accounting -> exactly one row carrying aggregate source counters
```

The materializer splits these row kinds without rerunning the source scan.
Use `(block_hash, transaction_index)` rather than `transaction_hash` alone for
same-transaction aggregation so historical duplicate transaction identifiers
cannot merge unrelated outputs.
Use named parameters:

```text
@cutoff_height
@cutoff_time
@window_30d_start
@window_90d_start
@window_365d_start
```

Do not embed a project id, cutoff, credential, or output path in SQL text.

- [ ] **Step 4: Implement the optional SDK adapter**

Keep `google.cloud.bigquery` imports inside `GoogleBigQueryBackend.__init__`.
When the optional extra is absent, return the safe error code
`bigquery_dependency_missing`. Configure every dry run with:

```python
job_config = bigquery.QueryJobConfig(
    dry_run=True,
    use_query_cache=False,
    query_parameters=parameters,
)
```

The source probe may fetch table metadata, issue BigQuery dry runs, and execute
only `source_checkpoint.sql` under its separate positive probe-byte cap. It
must not iterate full feature rows or create destination tables. The checkpoint
query is restricted to the latest seven completed UTC partitions and returns
at most one chain checkpoint row plus aggregate Taproot counters.

- [ ] **Step 5: Run BigQuery tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_bigquery_probe.py
```

Expected: all tests pass without network and without Google credentials.

- [ ] **Step 6: Commit Task 4**

```bash
git add src/crypto_address_identity/universe/query_plan.py \
  src/crypto_address_identity/universe/bigquery.py \
  src/crypto_address_identity/universe/sql/bigquery/address_features.sql \
  src/crypto_address_identity/universe/sql/bigquery/source_checkpoint.sql \
  tests/universe/fixtures/bigquery_schema.json \
  tests/universe/test_bigquery_probe.py
git commit -m "feat: add BigQuery BTC universe probe"
```

## Task 5: Build The Atomic Parquet And DuckDB Universe Store

**Files:**

- Create: `src/crypto_address_identity/universe/storage.py`
- Create: `src/crypto_address_identity/universe/sql/duckdb/schema.sql`
- Create: `tests/universe/test_storage.py`

- [ ] **Step 1: Write failing storage tests**

Test a staged campaign publish:

```python
def test_campaign_publish_is_atomic_and_checksum_pinned(tmp_path: Path) -> None:
    store = UniverseStore(tmp_path / "universe")
    writer = store.begin_campaign(manifest())
    writer.write_address_features(feature_rows())
    published = writer.publish()

    assert published.root.name == "btc-20260724"
    assert published.manifest_sha256 == manifest().manifest_sha256
    assert published.address_feature_rows == len(feature_rows())
    assert published.script_subject_rows == len(script_rows())
    assert not (tmp_path / "universe" / ".staging" / "btc-20260724").exists()
    assert store.verify("btc-20260724").status == "ok"
```

Also test:

- two identical address rows are rejected rather than silently deduplicated;
- two identical script ids are rejected rather than silently deduplicated;
- multi-address and nonstandard scripts remain stored with null address fields;
- an invalid canonical address blocks publish;
- a changed Parquet byte or manifest fails verification;
- a failed writer leaves no final campaign directory;
- every Parquet file uses the declared Arrow schema;
- DuckDB opens the final files read-only; and
- the identity SQLite path remains absent.

- [ ] **Step 2: Run storage tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_storage.py
```

Expected: missing `UniverseStore`.

- [ ] **Step 3: Implement the campaign layout**

Use:

```text
data/universe/campaigns/${CAMPAIGN_ID}/
  manifest.json
  source_probes/
    bigquery.json
    bitcoin_core.json
  address_features/
    prefix=00/part-00000.parquet
    prefix=01/part-00000.parquet
    prefix=ff/part-00000.parquet
  script_subjects/
    prefix=00/part-00000.parquet
    prefix=01/part-00000.parquet
    prefix=ff/part-00000.parquet
  calibration_anchors/
    anchors.parquet
  source_accounting/
    counters.parquet
  checksums.sha256
```

Partition address features by the first two lowercase hex characters of
`address_id`, and script subjects independently by the first two characters of
`script_id`. Write to a same-filesystem
`.staging/${CAMPAIGN_ID}-${UUID}` directory, fsync files,
write/check checksums, and atomically rename only after validation. The final
campaign is immutable: publishing a different manifest or bytes to an existing
campaign id raises `UniverseIntegrityError`.

- [ ] **Step 4: Implement read-only DuckDB views**

The schema SQL creates only read-side views over an explicit campaign path:

```sql
CREATE OR REPLACE VIEW universe_btc_address_feature AS
SELECT *
FROM read_parquet($address_feature_glob, hive_partitioning = true);

CREATE OR REPLACE VIEW universe_btc_script_subject AS
SELECT *
FROM read_parquet($script_subject_glob, hive_partitioning = true);

CREATE OR REPLACE VIEW universe_btc_source_accounting AS
SELECT *
FROM read_parquet($source_accounting_glob);
```

Do not attach or migrate `address_identity.sqlite3`. Use DuckDB
`read_only=True` for verification and candidate-statistics commands.

- [ ] **Step 5: Run storage tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_storage.py
```

Expected: all tests pass.

- [ ] **Step 6: Commit Task 5**

```bash
git add src/crypto_address_identity/universe/storage.py \
  src/crypto_address_identity/universe/sql/duckdb/schema.sql \
  tests/universe/test_storage.py
git commit -m "feat: add immutable BTC universe store"
```

## Task 6: Stream BigQuery Address Features Under An Explicit Byte Cap

**Files:**

- Modify: `src/crypto_address_identity/universe/bigquery.py`
- Create: `src/crypto_address_identity/universe/features.py`
- Create: `tests/universe/test_features.py`

- [ ] **Step 1: Write failing bounded-stream tests**

Test an iterator, not a list-returning API:

```python
def test_feature_materializer_streams_batches_and_accounts_every_source_row(
    tmp_path: Path,
) -> None:
    backend = FakeBigQueryBackend(
        dry_run_bytes=900,
        result_batches=[feature_batch_one(), feature_batch_two()],
        total_bytes_processed=900,
    )
    result = BigQueryFeatureMaterializer(
        backend=backend,
        store=UniverseStore(tmp_path / "universe"),
    ).run(
        request=materialization_request(maximum_bytes_billed=1_000),
    )

    assert result.status == "published"
    assert result.address_feature_rows == 4
    assert result.script_subject_rows == 6
    assert result.total_bytes_processed == 900
    assert result.provider_requests == 0
    assert result.provider_points == 0
```

Add tests proving:

- `maximum_bytes_billed=0` cannot execute;
- actual bytes above the dry-run estimate are reported but remain bounded by
  BigQuery's enforced cap;
- malformed/negative/out-of-range rows block final publish;
- duplicate `address_id` across batches blocks final publish;
- duplicate `script_id` across batches blocks final publish;
- a batch exception leaves no final campaign;
- result iteration uses bounded Arrow record batches; and
- no identity SQLite, raw 0xRouter path, or resolver export is created.

- [ ] **Step 2: Run feature tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_features.py
```

Expected: missing materializer failures.

- [ ] **Step 3: Add the execute-only backend method**

Define:

```python
class BigQueryBackend(Protocol):
    def table_metadata(self, table_id: str) -> TableMetadata:
        raise NotImplementedError

    def dry_run(
        self,
        sql: str,
        parameters: dict[str, object],
        maximum_bytes_billed: int,
    ) -> QueryEstimate:
        raise NotImplementedError

    def query_one(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
    ) -> Mapping[str, object]:
        raise NotImplementedError

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: dict[str, object],
        *,
        maximum_bytes_billed: int,
        page_size: int,
    ) -> Iterator[pyarrow.RecordBatch]:
        raise NotImplementedError
```

`stream_arrow_batches()` must set `maximum_bytes_billed`, disable legacy SQL,
use named parameters, and never retry a completed query. Transport retries may
only occur before a job id is accepted.

- [ ] **Step 4: Implement feature validation and publishing**

For every row:

1. For `script_subject`, decode `script_hex`, recompute `script_id`, and
   enforce the one-to-zero-or-one address mapping.
2. For `address_feature`, validate and normalize the address with
   `normalize_bitcoin_address()`.
3. Recompute `address_id`; reject mismatches.
4. Require all satoshi/count values to be integer and non-negative.
5. Require `current_utxo_sats == lifetime_received_sats - lifetime_spent_sats`.
6. Require `gross_flow_X == inflow_X + outflow_X`.
7. Require `first_seen_height <= last_seen_height <= cutoff_height`.
8. Require UTC-aware
   `first_seen_time <= last_seen_time <= campaign.cutoff_time`.
9. Split each validated Arrow batch into script, address-feature, and
   source-accounting Parquet writers.

Do not infer current UTXO from a row when the source query cannot prove exact
spent-input enrichment. Instead mark the source probe blocked.

- [ ] **Step 5: Run feature tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_features.py
```

Expected: all tests pass.

- [ ] **Step 6: Commit Task 6**

```bash
git add src/crypto_address_identity/universe/bigquery.py \
  src/crypto_address_identity/universe/features.py \
  tests/universe/test_features.py
git commit -m "feat: materialize bounded BTC address features"
```

## Task 7: Snapshot Read-Only Calibration Anchors

**Files:**

- Create: `src/crypto_address_identity/universe/anchors.py`
- Create: `tests/universe/test_anchors.py`

- [ ] **Step 1: Write failing anchor tests**

Create a migrated fixture identity database with one valid Tier A address, one
valid Tier B address, one Tier C-only address, one active conflict, one
`known_watchlist` candidate, and one stored entity-prediction address:

```python
def test_anchor_reader_exports_only_exact_strong_and_conflict_reasons(
    populated_identity_database: Path,
) -> None:
    snapshot = CalibrationAnchorReader(populated_identity_database).read(
        as_of=datetime(2026, 7, 24, tzinfo=UTC)
    )

    assert [(row.address_id, row.reason_code) for row in snapshot.rows] == [
        (ACTIVE_CONFLICT_ADDRESS_ID, "existing_provider_conflict"),
        (PREDICTED_ADDRESS_ID, "provider_entity_prediction"),
        (TIER_A_ADDRESS_ID, "official_or_signed_evidence"),
        (TIER_B_ADDRESS_ID, "official_or_signed_evidence"),
        (WATCHLIST_ADDRESS_ID, "existing_system_watchlist"),
    ]
    assert snapshot.database_sha256
```

Assert the reader:

- opens SQLite with `mode=ro&immutable=1`;
- does not call migrations;
- excludes Tier C/D/E-only evidence;
- excludes stale/revoked/expired Tier A/B evidence at `as_of`;
- includes only stored prediction rows and existing candidate provenance; it
  makes no provider call;
- emits only `address_id`, normalized address, and reason code;
- deterministically sorts and hashes the snapshot; and
- fails closed if the database changes between pre-read and post-read hashes.

- [ ] **Step 2: Run anchor tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_anchors.py
```

Expected: missing `CalibrationAnchorReader`.

- [ ] **Step 3: Implement the immutable reader**

Use a file descriptor/stat check plus SHA-256 before and after the read. Query
only active evidence/conflict rows as of the supplied UTC timestamp. Do not
copy source URLs, labels, entities, or review text into the universe campaign.

Define:

```python
class CalibrationAnchorRow(BaseModel):
    address_id: str
    normalized_address: str
    reason_code: Literal[
        "official_or_signed_evidence",
        "existing_provider_conflict",
        "provider_entity_prediction",
        "existing_system_watchlist",
    ]


class CalibrationAnchorSnapshot(BaseModel):
    as_of: datetime
    database_sha256: str
    rows: tuple[CalibrationAnchorRow, ...]
    snapshot_sha256: str
```

- [ ] **Step 4: Run anchor tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_anchors.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 7**

```bash
git add src/crypto_address_identity/universe/anchors.py \
  tests/universe/test_anchors.py
git commit -m "feat: snapshot BTC universe calibration anchors"
```

## Task 8: Implement The Versioned P0/P1 Selection Policy

**Files:**

- Create: `src/crypto_address_identity/universe/policy.py`
- Create: `tests/universe/test_policy.py`

- [ ] **Step 1: Write failing policy tests**

Use exact satoshi thresholds:

```python
BTC = 100_000_000


def test_p0_reasons_are_a_deduplicated_union() -> None:
    row = feature(
        current_utxo_sats=120 * BTC,
        max_same_tx_received_sats=700 * BTC,
        gross_flow_90d_sats=1_500 * BTC,
    )
    decision = BtcImportancePolicyV1().classify(row)

    assert decision.priority_class == "P0"
    assert decision.reason_codes == (
        "gross_90d_ge_1000_btc",
        "same_tx_receive_ge_500_btc",
        "utxo_ge_100_btc",
    )
    assert decision.unique_address_slots == 1
```

Test every score bucket, only-highest-bucket behavior, P1 threshold 25,
deterministic low-score control sampling, stable address-id tie breaking,
official/conflict forced reasons supplied separately from chain features, and
no ownership/entity fields in policy output.

- [ ] **Step 2: Run policy tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_policy.py
```

Expected: missing policy module.

- [ ] **Step 3: Implement `btc_importance_v1`**

Use integer satoshis only. Define:

```python
POLICY_VERSION = "btc_importance_v1"
P1_MIN_SCORE = 25

P0_THRESHOLDS = {
    "utxo_ge_100_btc": 100 * BTC,
    "same_tx_receive_ge_500_btc": 500 * BTC,
    "gross_90d_ge_1000_btc": 1_000 * BTC,
    "lifetime_ge_10000_active_365d": 10_000 * BTC,
}

BALANCE_BUCKETS = (
    (1_000 * BTC, 25),
    (100 * BTC, 20),
    (10 * BTC, 12),
    (1 * BTC, 5),
)
MAX_SAME_TX_RECEIPT_BUCKETS = (
    (5_000 * BTC, 25),
    (1_000 * BTC, 20),
    (500 * BTC, 18),
    (100 * BTC, 10),
)
GROSS_90D_BUCKETS = (
    (10_000 * BTC, 20),
    (1_000 * BTC, 15),
    (100 * BTC, 8),
    (10 * BTC, 3),
)
RECENCY_BUCKETS = (
    (30, 10),
    (90, 7),
    (365, 3),
)
DIRECT_LARGE_SELECTED_EDGE_POINTS = 10
PROVIDER_ENTITY_PREDICTION_POINTS = 15
EXISTING_SYSTEM_WATCHLIST_POINTS = 10
```

Within each tuple only the first matching descending threshold contributes.
`direct_large_counterparty_count > 0` contributes the edge points. A
checksum-pinned calibration reason contributes prediction/watchlist points;
the policy never queries SQLite or a provider itself. Evaluate recency and the
`lifetime_ge_10000_active_365d` P0 condition from
`campaign.cutoff_time - row.last_seen_time`, never from wall-clock time. Sort
reason codes and
cohort names lexicographically in serialized results. The deterministic control
sample uses the first 64 bits of `sha256(campaign_id + ":" + address_id)` and a
fixed 2% threshold; do not use process randomness.

- [ ] **Step 4: Run policy tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe/test_policy.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 8**

```bash
git add src/crypto_address_identity/universe/policy.py \
  tests/universe/test_policy.py
git commit -m "feat: add deterministic BTC importance policy"
```

## Task 9: Build Aggregate-Only First-Wave Statistics

**Files:**

- Create: `src/crypto_address_identity/universe/statistics.py`
- Create: `tests/universe/test_statistics.py`

- [ ] **Step 1: Write failing statistics tests**

Construct overlapping P0/P1 fixtures and assert:

```python
def test_statistics_report_exact_dedupe_and_first_wave_capacity(
    campaign: PublishedCampaign,
) -> None:
    result = CandidateStatisticsService(campaign).dry_run(
        runtime_minutes=480,
        requests_per_minute=25,
        estimated_points_per_address=1,
        discovery_point_budget=100_000,
    )

    assert result.status == "dry_run"
    assert result.rate_limited_capacity == 12_000
    assert result.provider_requests == 0
    assert result.provider_points == 0
    assert result.written_paths == ()
    assert result.reason_memberships > result.unique_selected_addresses
    assert result.duplicate_slots_prevented == (
        result.reason_memberships - result.unique_selected_addresses
    )
```

Require these output groups:

```text
source_coverage
quality_status
script_completeness
output_fact_materialized
unique_script_subjects
unique_standard_addresses
source_accounting_counts
calibration_anchor_count
anchor_only_count
p0_unique_addresses
p1_unique_addresses
control_unique_addresses
reason_memberships
reason_counts
cohort_counts
cohort_overlap_counts
duplicate_slots_prevented
rate_limited_capacity
point_limited_capacity
first_wave_unique_addresses
remaining_p0_addresses
projected_minimum_minutes
provider_requests
provider_points
written_paths
```

Add tests for 40% cohort caps, unused-quota reassignment, all-P0 precedence,
stable ordering across repeated runs, incomplete-script warning propagation,
blocked source quality, anchor-only P0 accounting, and absence of address
values in JSON.

- [ ] **Step 2: Run statistics tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_statistics.py
```

Expected: missing service failures.

- [ ] **Step 3: Implement one-pass aggregate statistics**

Open the campaign through a read-only DuckDB connection. Classify rows in
streaming batches and retain only counters plus the bounded first-wave heap;
never load the complete address universe into Python memory.

Union checksum-pinned calibration anchors by `address_id`. An anchor absent
from the observed chain feature table remains an `anchor_only` P0 calibration
candidate and is counted separately; it does not receive invented balances,
flow values, or chain-derived score points.

Calculate:

```python
rate_limited_capacity = requests_per_minute * runtime_minutes
point_limited_capacity = discovery_point_budget // estimated_points_per_address
first_wave_capacity = min(
    unique_due_addresses,
    rate_limited_capacity,
    point_limited_capacity,
)
projected_minimum_minutes = math.ceil(
    unique_due_addresses / requests_per_minute
)
```

When `estimated_points_per_address` is unavailable before the canary, emit
`point_limited_capacity=null` and use only the rate-limited projection. Never
invent a one-point assumption in production output.

Fill the bounded first-wave heap in this order:

1. all P0 candidates, ordered by forced-evidence/conflict priority, economic
   magnitude, then `address_id`;
2. remaining P1 slots with 30% current-capital, 25% historical large-receipt,
   20% high-turnover, 10% dormant-holder, 10% high-value-connector, and 5%
   calibration/control quotas; and
3. reassign unused quota only after every other cohort is exhausted.

An address consumes one slot even when it belongs to several cohorts. No
single P1 cohort may consume more than 40% of P1 slots unless every other due
cohort is exhausted.

Use this P0 reason precedence:

```text
existing_provider_conflict
official_or_signed_evidence
utxo_ge_100_btc
same_tx_receive_ge_500_btc
gross_90d_ge_1000_btc
lifetime_ge_10000_active_365d
```

For an address with several P0 reasons, use its earliest precedence reason.
Within a reason, sort descending by that reason's satoshi metric; forced
conflict/evidence rows have no invented metric and sort by `address_id`.

- [ ] **Step 4: Run statistics tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_statistics.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 9**

```bash
git add src/crypto_address_identity/universe/statistics.py \
  tests/universe/test_statistics.py
git commit -m "feat: add BTC first-wave dry-run statistics"
```

## Task 10: Add Fail-Closed Universe CLI Commands

**Files:**

- Modify: `src/crypto_address_identity/cli.py`
- Create: `tests/universe/test_cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write failing CLI tests**

Add commands:

```text
cai universe probe bigquery --dry-run --as-of-date YYYY-MM-DD
cai universe probe bigquery --execute-readonly --as-of-date YYYY-MM-DD --maximum-bytes-billed N
cai universe probe bitcoin-core --execute-readonly
cai universe build bigquery --dry-run --campaign-id ID --cutoff-height H --cutoff-time ISO8601
cai universe build bigquery --execute-chain-read --campaign-id ID --cutoff-height H --cutoff-time ISO8601 --maximum-bytes-billed N
cai universe candidates --campaign-id ID --dry-run --runtime-minutes 480 --requests-per-minute 25
```

Test that:

- `universe probe bigquery --dry-run` performs no network and writes nothing;
- `universe probe bigquery --execute-readonly` can use injected fake backends;
- BigQuery execute-readonly is rejected without a positive
  `--maximum-bytes-billed`;
- `universe build bigquery --dry-run` returns the exact query hash and dry-run
  bytes;
- `--execute-chain-read` is rejected when `N <= 0`;
- candidate dry-run does not require `CAI_0XROUTER_TOKEN`;
- candidate dry-run does not open or migrate the identity SQLite database;
- output contains `provider_requests=0` and `provider_points=0`;
- output contains no addresses, RPC cookie, authorization data, GCP credential
  path, or raw exception; and
- mutually exclusive `--dry-run`, `--execute-readonly`, and
  `--execute-chain-read` modes fail with `invalid_input`.

- [ ] **Step 2: Run CLI tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_cli.py tests/test_cli.py
```

Expected: parser failures for the missing `universe` command.

- [ ] **Step 3: Implement the command handlers**

Keep handler construction injectable through small factory functions so tests
do not patch global SDK objects. Return structured JSON only. Add safe error
codes:

```text
bigquery_dependency_missing
bigquery_credentials_unavailable
bigquery_schema_blocked
bigquery_budget_exceeded
bitcoin_rpc_unavailable
bitcoin_rpc_unsafe_url
bitcoin_source_blocked
universe_integrity_error
campaign_not_found
candidate_stats_blocked
```

Do not catch and echo provider/SDK/RPC exception strings.

- [ ] **Step 4: Add a provider-boundary regression test**

Monkeypatch `ZeroXRouterClient.__init__` to raise and prove every universe CLI
path still succeeds against fixtures. Assert the coverage LaunchAgent files are
byte-identical before and after CLI tests.

- [ ] **Step 5: Run CLI tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_cli.py tests/test_cli.py tests/providers
```

Expected: all tests pass.

- [ ] **Step 6: Commit Task 10**

```bash
git add src/crypto_address_identity/cli.py \
  tests/universe/test_cli.py tests/test_cli.py
git commit -m "feat: expose provider-free BTC universe CLI"
```

## Task 11: Document Source Cost, Integrity, And Approval Gates

**Files:**

- Modify: `README.md`
- Modify: `docs/btc_identity_operations.md`
- Modify: `conf/env/address_identity.env.example`
- Create: `tests/universe/test_docs_contract.py`

- [ ] **Step 1: Write failing documentation contract tests**

Assert documentation contains:

```python
def test_phase_one_docs_state_the_provider_free_boundary() -> None:
    operations = OPERATIONS.read_text(encoding="utf-8")
    assert "provider_requests=0" in operations
    assert "--execute-chain-read" in operations
    assert "--maximum-bytes-billed" in operations
    assert "does not approve the 1,000-address canary" in operations
    assert "BigQuery free tier is account-wide" in operations
```

Also assert the environment example contains no non-empty GCP credential,
Bitcoin RPC password, cookie content, or 0xRouter token.

- [ ] **Step 2: Run docs tests and verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_docs_contract.py
```

Expected: missing Phase 1 documentation statements.

- [ ] **Step 3: Document the operating sequence**

Document:

1. Offline configuration validation.
2. BigQuery metadata/dry-run probe.
3. Bitcoin Core read-only probe.
4. Cutoff height/hash reconciliation.
5. Review of dry-run bytes and account-wide BigQuery allowance.
6. Separately approved chain read with exact byte cap.
7. Campaign checksum verification.
8. Aggregate-only candidate dry-run.
9. Stop and report; no canary.

State that a public BigQuery dataset is not automatically free. The command
reports bytes but cannot know the account's remaining free-tier allowance.
State that a pruned Bitcoin Core node cannot prove historical script coverage.

- [ ] **Step 4: Run docs tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/universe/test_docs_contract.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit Task 11**

```bash
git add README.md docs/btc_identity_operations.md \
  conf/env/address_identity.env.example tests/universe/test_docs_contract.py
git commit -m "docs: add BTC universe phase one operations"
```

## Task 12: Full Local Verification And Review

**Files:**

- Review all files changed by Tasks 1-11.

- [ ] **Step 1: Run targeted universe tests**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe
```

Expected: all tests pass.

- [ ] **Step 2: Run existing provider and resolver regressions**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/providers tests/evidence tests/resolver tests/consumers tests/ops
```

Expected: all tests pass; existing coverage-sync behavior is unchanged.

- [ ] **Step 3: Run the full suite and compile checks**

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
PYTHONPYCACHEPREFIX=/private/tmp/cai-pycache \
  /Users/barry/.pyenv/versions/3.13.7/bin/python3 -m compileall -q \
  src/crypto_address_identity
git diff --check
```

Expected: all commands exit zero.

- [ ] **Step 4: Run the no-provider and no-secret review**

Verify:

```bash
rg -n "ZeroXRouterClient|CAI_0XROUTER_TOKEN|/chaindata" \
  src/crypto_address_identity/universe tests/universe
```

Expected: no runtime import/use; documentation tests may mention the token name
only to assert absence. Run a count-only secret scan without printing values.

- [ ] **Step 5: Review invariants**

Confirm:

- BigQuery execute mode always has `maximum_bytes_billed > 0`.
- Source probes and dry runs do not write campaign or identity state.
- A campaign publishes only after checksums and row-level gates pass.
- Candidate statistics expose no address list.
- The existing identity SQLite schema is untouched.
- Existing provider, coverage-sync, resolver, and consumer code behavior is
  unchanged.
- No source result claims script completeness when raw `script_hex` is absent.
- A script-complete campaign has exactly one stored row per distinct
  `script_id`, including non-provider-enrichable scripts.

- [ ] **Step 6: Commit any review-only fixes**

Stage only Phase 1 files and use a focused commit message:

```bash
git add pyproject.toml conf/env/address_identity.env.example README.md \
  docs/btc_identity_operations.md src/crypto_address_identity/cli.py \
  src/crypto_address_identity/core/config.py \
  src/crypto_address_identity/universe tests/conftest.py tests/test_cli.py \
  tests/test_config.py tests/universe
git commit -m "fix: harden BTC universe phase one gates"
```

Skip this commit when review produces no changes.

## Task 13: Controlled Source Probe And Real-Scale Statistics Gate

This task is operational and requires a separate explicit approval after the
implementation commit has entered the reviewed branch. It still makes no
identity-provider call.

**Files written at runtime only:**

- `data/universe/campaigns/${CAMPAIGN_ID}/`

- [ ] **Step 1: Verify no recurring coverage task is running**

Read-only check the LaunchAgent state. If loaded, require it to be inactive
before any chain read. Do not unload or modify it without separate operational
approval.

- [ ] **Step 2: Run offline probe plans**

```bash
PROBE_AS_OF_DATE="$(date -u +%F)"
PYTHONPATH=src python -m crypto_address_identity universe probe bigquery \
  --dry-run \
  --as-of-date "$PROBE_AS_OF_DATE"
```

Expected:

```text
status=dry_run
network_requests=0
written_paths=[]
provider_requests=0
provider_points=0
```

- [ ] **Step 3: Run read-only source probes**

Run the BigQuery metadata/dry-run probe and Bitcoin Core probe with
`--execute-readonly`. Record only aggregate/source facts:

```bash
: "${APPROVED_PROBE_BYTES:?export the reviewed positive checkpoint-query cap}"
PYTHONPATH=src python -m crypto_address_identity universe probe bigquery \
  --execute-readonly \
  --as-of-date "$PROBE_AS_OF_DATE" \
  --maximum-bytes-billed "$APPROVED_PROBE_BYTES"

PYTHONPATH=src python -m crypto_address_identity universe probe bitcoin-core \
  --execute-readonly
```

```text
schema_sha256
query_sha256
dry_run_bytes
latest_height/time
finalized_height/hash
script_completeness
capabilities
blocking_reasons
warnings
```

Do not print credentials, cookie paths/content, project credential files, or
raw RPC/SDK errors.

- [ ] **Step 4: Reconcile the cutoff**

Require the BigQuery cutoff height/hash and Bitcoin Core finalized checkpoint to
match exactly. If Bitcoin Core is unavailable or pruned, report
`reconciliation_status=partial` and stop before claiming canonical script
coverage.

- [ ] **Step 5: Review BigQuery cost before executing**

Compare `dry_run_bytes` with:

- the operator-supplied job cap;
- the project's remaining account-wide free-tier allowance; and
- local storage/transfer capacity.

Do not proceed merely because `dry_run_bytes` is below 1 TiB. Approval must name
the exact query hash, cutoff, and `maximum_bytes_billed`.

- [ ] **Step 6: Execute one bounded chain read after approval**

```bash
: "${REVIEWED_CUTOFF_HEIGHT:?export the accepted reconciled cutoff height}"
: "${REVIEWED_CUTOFF_TIME:?export the accepted reconciled UTC cutoff time}"
: "${APPROVED_MAXIMUM_BYTES_BILLED:?export the reviewed positive byte cap}"
CAMPAIGN_ID="btc-$(date -u +%Y%m%d)-cutoff-${REVIEWED_CUTOFF_HEIGHT}"
PYTHONPATH=src python -m crypto_address_identity universe build bigquery \
  --execute-chain-read \
  --campaign-id "$CAMPAIGN_ID" \
  --cutoff-height "$REVIEWED_CUTOFF_HEIGHT" \
  --cutoff-time "$REVIEWED_CUTOFF_TIME" \
  --maximum-bytes-billed "$APPROVED_MAXIMUM_BYTES_BILLED"
```

Stop on schema drift, budget rejection, row-quality failure, source drift, or
checksum failure. Do not retry a completed BigQuery job.

- [ ] **Step 7: Verify the immutable campaign**

Run campaign verification read-only and record:

```text
manifest_sha256
source_manifest_sha256
script_subject_rows
address_feature_rows
unique_standard_addresses
empty_address_rows
multi_address_rows
nonstandard_script_rows
invalid_address_rows
parquet_file_count
parquet_total_bytes
```

- [ ] **Step 8: Run first-wave aggregate statistics dry-run**

```bash
PYTHONPATH=src python -m crypto_address_identity universe candidates \
  --campaign-id "$CAMPAIGN_ID" \
  --dry-run \
  --runtime-minutes 480 \
  --requests-per-minute 25
```

Expected:

```text
provider_requests=0
provider_points=0
written_paths=[]
```

Record P0/P1/control counts, overlap, deduplication, first-wave capacity, and
remaining P0 addresses. Do not emit or save address values.

- [ ] **Step 9: Stop before canary**

Prepare a review report containing the real universe size, source gaps,
BigQuery bytes, local storage bytes, cohort counts, overlaps, and projected
minimum provider runtime. The report must explicitly state:

```text
canary_approved=false
provider_calls_executed=0
provider_points_spent=0
```

The next implementation/operations plan may propose the 10-address capability
probe and 1,000-address canary only after this report is accepted.

## Test Commands

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/universe
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/providers tests/evidence tests/resolver tests/consumers tests/ops
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
PYTHONPYCACHEPREFIX=/private/tmp/cai-pycache \
  /Users/barry/.pyenv/versions/3.13.7/bin/python3 -m compileall -q \
  src/crypto_address_identity
git diff --check
```

## Acceptance Criteria

- BigQuery dry-run mode reports exact schema/query hashes and estimated bytes
  without executing the full data query; execute-readonly mode may run only the
  bounded recent-partition checkpoint query under its own positive byte cap.
- Bitcoin Core probe uses only read-only RPC methods and never exposes cookie
  content.
- BigQuery execute mode is impossible without an explicit positive byte cap.
- Accepted BigQuery campaigns retain every distinct `script_hex` as a
  checksum-derived script subject, enforce one-address mapping, preserve
  integer satoshis, and publish exact source-accounting counters.
- Phase 1 reports `output_fact_materialized=false`; it does not claim the later
  Bitcoin Core per-output history has been built.
- Large universe data is partitioned Parquet/DuckDB state, not identity SQLite.
- Every final campaign file is checksum-pinned and published atomically.
- Candidate selection implements `btc_importance_v1`, exact P0/P1 thresholds,
  deterministic control sampling, diversity quotas, and stable tie breaking.
- Aggregate dry-run statistics expose real cohort scale and overlap without
  emitting addresses or writing candidate rows.
- All Phase 1 output reports `provider_requests=0` and `provider_points=0`.
- Existing coverage-sync, provider fetch, evidence, resolver, LaunchAgent, and
  consumer behavior remain unchanged.
- No canary or provider enrichment is approved by completing this plan.
