"""Validate and publish immutable Strict V2-S candidate artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import uuid
from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import Field, field_validator, model_validator

from crypto_address_identity.chains.bitcoin import normalize_bitcoin_address
from crypto_address_identity.universe.candidate_materialization_execution_v2_s import (
    STRICT_V2_S_DESTINATION_TABLE_ID,
    STRICT_V2_S_EXECUTION_RECEIPT_VERSION,
)
from crypto_address_identity.universe.candidate_materialization_v2_s import (
    EXPECTED_STRICT_V2_S_COARSE_COUNT,
    EXPECTED_STRICT_V2_S_COARSE_OTHER_COUNT,
    EXPECTED_STRICT_V2_S_EDGE_COUNT,
    EXPECTED_STRICT_V2_S_P0_COUNT,
    EXPECTED_STRICT_V2_S_P1_COUNT,
    PINNED_STRICT_V2_S_QUERY_SHA256,
    STRICT_V2_S_CANDIDATE_SCHEMA_SHA256,
    STRICT_V2_S_CUTOFF_HEIGHT,
    STRICT_V2_S_CUTOFF_TIME,
)
from crypto_address_identity.universe.models import UniverseModel


STRICT_V2_S_ARTIFACT_SCHEMA_VERSION = (
    "btc_strict_v2_s_candidate_artifact_v1"
)
STRICT_V2_S_PRODUCTION_CAMPAIGN_ID = "btc-v2s-bootstrap-959187"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAMPAIGN_RE = re.compile(
    r"^[a-z0-9][a-z0-9._-]{2,127}$"
)
_TIERS = ("p0", "p1", "edge", "coarse_other")
_TIER_RANKS = {tier: index for index, tier in enumerate(_TIERS)}
_SATS_FIELDS = (
    "current_utxo_sats",
    "lifetime_received_sats",
    "residual_gross_90d_sats",
    "max_same_tx_received_lifetime_sats",
    "max_same_tx_received_365d_sats",
    "max_same_tx_received_90d_sats",
)
_COUNT_FIELDS = (
    "same_tx_receive_ge_500_btc_90d_count",
    "same_tx_receive_ge_500_btc_365d_count",
    "active_tx_90d_count",
    "active_day_90d_count",
    "active_tx_365d_count",
    "active_day_365d_count",
)
_HASH_FIELDS = (
    "normalized_address",
    "candidate_tier",
    "tier_rank",
    "address_bucket",
    "v2_chain_score",
    "strict_p0_mask",
    "receipt_support_mask",
    *_SATS_FIELDS,
    *_COUNT_FIELDS,
    "last_seen_time",
)

CANDIDATE_ARTIFACT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("normalized_address", pa.string(), nullable=False),
        pa.field("candidate_tier", pa.string(), nullable=False),
        pa.field("tier_rank", pa.int64(), nullable=False),
        pa.field("address_bucket", pa.int64(), nullable=False),
        pa.field("v2_chain_score", pa.int64(), nullable=False),
        pa.field("strict_p0_mask", pa.int64(), nullable=False),
        pa.field("receipt_support_mask", pa.int64(), nullable=False),
        *[
            pa.field(name, pa.decimal128(38, 0), nullable=False)
            for name in _SATS_FIELDS
        ],
        *[
            pa.field(name, pa.int64(), nullable=False)
            for name in _COUNT_FIELDS
        ],
        pa.field(
            "last_seen_time",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
        ),
        pa.field("candidate_row_sha256", pa.string(), nullable=False),
    ]
)


class CandidatePublicationError(RuntimeError):
    """Raised before an incomplete candidate artifact can publish."""


@dataclass(frozen=True)
class CandidateArtifactExpectedCounts:
    total: int = EXPECTED_STRICT_V2_S_COARSE_COUNT
    p0: int = EXPECTED_STRICT_V2_S_P0_COUNT
    p1: int = EXPECTED_STRICT_V2_S_P1_COUNT
    edge: int = EXPECTED_STRICT_V2_S_EDGE_COUNT
    coarse_other: int = EXPECTED_STRICT_V2_S_COARSE_OTHER_COUNT

    def __post_init__(self) -> None:
        values = (
            self.total,
            self.p0,
            self.p1,
            self.edge,
            self.coarse_other,
        )
        if any(isinstance(value, bool) or value < 0 for value in values):
            raise ValueError("candidate counts must be non-negative")
        if self.total != self.p0 + self.p1 + self.edge + self.coarse_other:
            raise ValueError("candidate tier counts must reconcile")

    def by_tier(self) -> dict[str, int]:
        return {
            "p0": self.p0,
            "p1": self.p1,
            "edge": self.edge,
            "coarse_other": self.coarse_other,
        }


class StrictV2SCandidateRow(UniverseModel):
    normalized_address: str
    candidate_tier: Literal["p0", "p1", "edge", "coarse_other"]
    tier_rank: int = Field(ge=0, le=3)
    address_bucket: int = Field(ge=0, le=63)
    v2_chain_score: int = Field(ge=0)
    strict_p0_mask: int = Field(ge=0, le=15)
    receipt_support_mask: int = Field(ge=0, le=7)
    current_utxo_sats: Decimal
    lifetime_received_sats: Decimal
    residual_gross_90d_sats: Decimal
    max_same_tx_received_lifetime_sats: Decimal
    max_same_tx_received_365d_sats: Decimal
    max_same_tx_received_90d_sats: Decimal
    same_tx_receive_ge_500_btc_90d_count: int = Field(ge=0)
    same_tx_receive_ge_500_btc_365d_count: int = Field(ge=0)
    active_tx_90d_count: int = Field(ge=0)
    active_day_90d_count: int = Field(ge=0)
    active_tx_365d_count: int = Field(ge=0)
    active_day_365d_count: int = Field(ge=0)
    last_seen_time: datetime
    candidate_row_sha256: str

    @field_validator(*_SATS_FIELDS)
    @classmethod
    def validate_sats(cls, value: Decimal) -> Decimal:
        if (
            value.is_nan()
            or value.is_infinite()
            or value != value.to_integral_value()
            or value < 0
            or value >= Decimal(10) ** 38
        ):
            raise ValueError("candidate satoshi field is invalid")
        return value

    @field_validator("last_seen_time")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("candidate timestamp must be timezone-aware")
        value = value.astimezone(UTC)
        if value > STRICT_V2_S_CUTOFF_TIME:
            raise ValueError("candidate timestamp exceeds cutoff")
        return value

    @field_validator("candidate_row_sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("candidate row checksum must be SHA-256")
        return value

    @model_validator(mode="after")
    def validate_policy(self) -> "StrictV2SCandidateRow":
        subject = normalize_bitcoin_address(self.normalized_address)
        if subject.normalized_address != self.normalized_address:
            raise ValueError("candidate address is not canonical")
        if self.address_bucket != candidate_address_bucket(
            self.normalized_address
        ):
            raise ValueError("candidate address bucket is invalid")
        if self.current_utxo_sats > self.lifetime_received_sats:
            raise ValueError("candidate UTXO exceeds lifetime receipts")
        if not (
            self.max_same_tx_received_90d_sats
            <= self.max_same_tx_received_365d_sats
            <= self.max_same_tx_received_lifetime_sats
            <= self.lifetime_received_sats
        ):
            raise ValueError("candidate receipt maxima are inconsistent")
        if not (
            self.same_tx_receive_ge_500_btc_90d_count
            <= self.same_tx_receive_ge_500_btc_365d_count
            <= self.active_tx_365d_count
        ):
            raise ValueError("candidate receipt counts are inconsistent")
        if (
            self.same_tx_receive_ge_500_btc_90d_count
            > self.active_tx_90d_count
            or self.active_tx_90d_count > self.active_tx_365d_count
            or self.active_day_90d_count > self.active_tx_90d_count
            or self.active_day_365d_count > self.active_tx_365d_count
            or self.active_day_90d_count > self.active_day_365d_count
        ):
            raise ValueError("candidate activity counts are inconsistent")
        expected = _classify_candidate(self)
        if self.v2_chain_score != expected["v2_chain_score"]:
            raise ValueError("candidate score is invalid")
        if self.strict_p0_mask != expected["strict_p0_mask"]:
            raise ValueError("candidate P0 mask is invalid")
        if self.receipt_support_mask != expected["receipt_support_mask"]:
            raise ValueError("candidate receipt mask is invalid")
        if self.candidate_tier != expected["candidate_tier"]:
            raise ValueError("candidate tier is invalid")
        if self.tier_rank != _TIER_RANKS[self.candidate_tier]:
            raise ValueError("candidate tier rank is invalid")
        if self.candidate_row_sha256 != candidate_row_sha256(
            self.model_dump(mode="python")
        ):
            raise ValueError("candidate row checksum is invalid")
        return self

    def arrow_row(self) -> dict[str, object]:
        values = self.model_dump(mode="python")
        for field_name in _SATS_FIELDS:
            values[field_name] = Decimal(int(values[field_name]))
        return values


class StrictV2SCandidatePublicationRequest(UniverseModel):
    campaign_id: str
    destination_table_id: str
    source_execution_receipt_path: Path
    expected_execution_receipt_sha256: str
    artifact_root: Path
    expected_result_schema_sha256: str
    page_size: int = Field(default=10_000, ge=1, le=100_000)

    @field_validator("campaign_id")
    @classmethod
    def validate_campaign(cls, value: str) -> str:
        if not _CAMPAIGN_RE.fullmatch(value):
            raise ValueError("candidate campaign id is invalid")
        return value

    @field_validator(
        "expected_execution_receipt_sha256",
        "expected_result_schema_sha256",
    )
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("publication checksum must be SHA-256")
        return value

    @model_validator(mode="after")
    def validate_contract(self) -> "StrictV2SCandidatePublicationRequest":
        if self.destination_table_id != STRICT_V2_S_DESTINATION_TABLE_ID:
            raise ValueError("publication destination table is not frozen")
        if (
            self.expected_result_schema_sha256
            != STRICT_V2_S_CANDIDATE_SCHEMA_SHA256
        ):
            raise ValueError("publication result schema is not frozen")
        return self


class StrictV2SCandidatePublicationOutcome(UniverseModel):
    status: Literal["dry_run", "published"]
    campaign_id: str
    campaign_root: str
    destination_table_id: str
    candidate_rows: int = Field(ge=0)
    tier_counts: dict[str, int]
    source_execution_receipt_sha256: str
    result_schema_sha256: str
    manifest_sha256: str | None = None
    network_requests: int = Field(ge=0)
    provider_requests: Literal[0] = 0
    provider_points: Literal[0] = 0
    written_paths: tuple[str, ...] = ()


class CandidateTableBackend(Protocol):
    def destination_metadata(self, table_id: str): ...

    def stream_destination_arrow_batches(
        self,
        *,
        table_id: str,
        page_size: int,
    ) -> Iterator[pa.RecordBatch]: ...


class StrictV2SCandidateArtifactPublisher:
    """Stream, validate, partition, and atomically publish candidate rows."""

    def __init__(
        self,
        *,
        backend: CandidateTableBackend | None = None,
        expected_counts: CandidateArtifactExpectedCounts | None = None,
    ) -> None:
        self._backend = backend
        self._expected_counts = (
            expected_counts or CandidateArtifactExpectedCounts()
        )

    @staticmethod
    def preview(
        request: StrictV2SCandidatePublicationRequest,
    ) -> StrictV2SCandidatePublicationOutcome:
        receipt = _read_execution_receipt(request)
        return StrictV2SCandidatePublicationOutcome(
            status="dry_run",
            campaign_id=request.campaign_id,
            campaign_root=str(
                request.artifact_root / "campaigns" / request.campaign_id
            ),
            destination_table_id=request.destination_table_id,
            candidate_rows=0,
            tier_counts={tier: 0 for tier in _TIERS},
            source_execution_receipt_sha256=(
                request.expected_execution_receipt_sha256
            ),
            result_schema_sha256=(
                request.expected_result_schema_sha256
            ),
            network_requests=0,
        )

    def run(
        self,
        request: StrictV2SCandidatePublicationRequest,
    ) -> StrictV2SCandidatePublicationOutcome:
        if self._backend is None:
            raise CandidatePublicationError(
                "candidate publication backend is unavailable"
            )
        receipt = _read_execution_receipt(request)
        if int(receipt["candidate_rows"]) != self._expected_counts.total:
            raise CandidatePublicationError(
                "execution receipt candidate count does not match"
            )
        final_root = (
            request.artifact_root / "campaigns" / request.campaign_id
        )
        if final_root.exists():
            raise CandidatePublicationError(
                "candidate campaign already exists"
            )
        staging = (
            request.artifact_root
            / ".staging"
            / f"{request.campaign_id}-{uuid.uuid4().hex}"
        )
        shard_root = staging / ".shards"
        candidate_root = staging / "candidates"
        try:
            staging.mkdir(parents=True, mode=0o700)
            shard_root.mkdir(mode=0o700)
            candidate_root.mkdir(mode=0o700)
            metadata = self._backend.destination_metadata(
                request.destination_table_id
            )
            _validate_destination_metadata(
                metadata,
                request=request,
                expected_rows=self._expected_counts.total,
            )
            counts = {tier: 0 for tier in _TIERS}
            shard_numbers: dict[tuple[str, int], int] = defaultdict(int)
            total = 0
            seen_path = staging / ".seen_addresses.sqlite3"
            seen = sqlite3.connect(seen_path)
            seen_path.chmod(0o600)
            try:
                seen.execute(
                    "CREATE TABLE seen_address ("
                    "normalized_address TEXT PRIMARY KEY"
                    ") WITHOUT ROWID"
                )
                source_batches = (
                    self._backend.stream_destination_arrow_batches(
                        table_id=request.destination_table_id,
                        page_size=request.page_size,
                    )
                )
                for source_batch in source_batches:
                    if not isinstance(source_batch, pa.RecordBatch):
                        raise CandidatePublicationError(
                            "candidate stream yielded a non-Arrow batch"
                        )
                    for batch in _bounded_record_batches(
                        source_batch,
                        request.page_size,
                    ):
                        grouped: dict[
                            tuple[str, int],
                            list[dict[str, object]],
                        ] = defaultdict(list)
                        batch_addresses: list[tuple[str]] = []
                        for raw in batch.to_pylist():
                            row = StrictV2SCandidateRow.model_validate(raw)
                            grouped[
                                (row.candidate_tier, row.address_bucket)
                            ].append(row.arrow_row())
                            batch_addresses.append(
                                (row.normalized_address,)
                            )
                            counts[row.candidate_tier] += 1
                            total += 1
                        try:
                            seen.executemany(
                                "INSERT INTO seen_address"
                                "(normalized_address) VALUES (?)",
                                batch_addresses,
                            )
                            seen.commit()
                        except sqlite3.IntegrityError as exc:
                            seen.rollback()
                            raise CandidatePublicationError(
                                "duplicate candidate address detected"
                            ) from exc
                        for key, rows in grouped.items():
                            tier, bucket = key
                            part = shard_numbers[key]
                            shard_numbers[key] += 1
                            path = (
                                shard_root
                                / f"tier={tier}"
                                / f"bucket={bucket:02d}"
                                / f"part-{part:05d}.parquet"
                            )
                            path.parent.mkdir(
                                parents=True,
                                exist_ok=True,
                                mode=0o700,
                            )
                            _write_parquet(path, rows)
            finally:
                seen.close()
            seen_path.unlink()

            if total != self._expected_counts.total:
                raise CandidatePublicationError(
                    "candidate artifact total count mismatch"
                )
            if counts != self._expected_counts.by_tier():
                raise CandidatePublicationError(
                    "candidate artifact tier count mismatch"
                )
            files = self._compact_shards(
                shard_root=shard_root,
                candidate_root=candidate_root,
            )
            shutil.rmtree(shard_root)
            receipt_copy = staging / "execution_receipt.json"
            _copy_checksum_pinned_file(
                request.source_execution_receipt_path,
                receipt_copy,
                request.expected_execution_receipt_sha256,
            )
            files.append(_file_record(receipt_copy, staging, None))
            manifest = {
                "schema_version": STRICT_V2_S_ARTIFACT_SCHEMA_VERSION,
                "campaign_id": request.campaign_id,
                "policy_version": "btc_importance_v2",
                "variant": "V2-S",
                "cutoff_height": STRICT_V2_S_CUTOFF_HEIGHT,
                "cutoff_time": _iso(STRICT_V2_S_CUTOFF_TIME),
                "query_sha256": PINNED_STRICT_V2_S_QUERY_SHA256,
                "result_schema_sha256": (
                    request.expected_result_schema_sha256
                ),
                "source_execution_receipt_sha256": (
                    request.expected_execution_receipt_sha256
                ),
                "destination_table_id": request.destination_table_id,
                "candidate_rows": total,
                "tier_counts": counts,
                "provider_requests": 0,
                "provider_points": 0,
                "files": sorted(
                    files,
                    key=lambda item: str(item["path"]),
                ),
            }
            manifest_sha256 = _json_sha256(manifest)
            manifest["manifest_sha256"] = manifest_sha256
            manifest_path = staging / "manifest.json"
            _write_json_file(manifest_path, manifest)
            final_root.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.replace(staging, final_root)
        except Exception as exc:
            shutil.rmtree(staging, ignore_errors=True)
            if isinstance(exc, CandidatePublicationError):
                raise
            raise CandidatePublicationError(
                "candidate artifact publication failed"
            ) from exc
        return StrictV2SCandidatePublicationOutcome(
            status="published",
            campaign_id=request.campaign_id,
            campaign_root=str(final_root),
            destination_table_id=request.destination_table_id,
            candidate_rows=total,
            tier_counts=counts,
            source_execution_receipt_sha256=(
                request.expected_execution_receipt_sha256
            ),
            result_schema_sha256=(
                request.expected_result_schema_sha256
            ),
            manifest_sha256=manifest_sha256,
            network_requests=2,
            written_paths=(str(final_root),),
        )

    @staticmethod
    def _compact_shards(
        *,
        shard_root: Path,
        candidate_root: Path,
    ) -> list[dict[str, object]]:
        files: list[dict[str, object]] = []
        for tier in _TIERS:
            tier_root = shard_root / f"tier={tier}"
            if not tier_root.exists():
                continue
            for bucket_root in sorted(tier_root.glob("bucket=*")):
                shards = sorted(bucket_root.glob("*.parquet"))
                if not shards:
                    continue
                # Read files directly so Hive-style parent directories do not
                # inject synthetic ``tier`` and ``bucket`` columns.
                tables = [pq.ParquetFile(path).read() for path in shards]
                combined = pa.concat_tables(tables)
                order = pc.sort_indices(
                    combined,
                    sort_keys=[("normalized_address", "ascending")],
                )
                sorted_table = pc.take(combined, order)
                addresses = sorted_table["normalized_address"].to_pylist()
                if any(
                    left == right
                    for left, right in zip(addresses, addresses[1:])
                ):
                    raise CandidatePublicationError(
                        "duplicate candidate address detected"
                    )
                target = (
                    candidate_root
                    / f"tier={tier}"
                    / bucket_root.name
                    / "part-00000.parquet"
                )
                target.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                    mode=0o700,
                )
                pq.write_table(
                    sorted_table.cast(CANDIDATE_ARTIFACT_ARROW_SCHEMA),
                    target,
                    compression="zstd",
                    version="2.6",
                    write_statistics=True,
                )
                target.chmod(0o600)
                files.append(
                    _file_record(
                        target,
                        candidate_root.parent,
                        sorted_table.num_rows,
                    )
                )
        return files


def candidate_address_bucket(normalized_address: str) -> int:
    digest = hashlib.sha256(normalized_address.encode("ascii")).digest()
    return int.from_bytes(digest[:4], "big") % 64


def candidate_row_sha256(values: Mapping[str, object]) -> str:
    encoded_fields: list[str] = [
        "btc_strict_v2_s_candidate_row_v1",
    ]
    for name in _HASH_FIELDS:
        value = values[name]
        if name in _SATS_FIELDS:
            value = str(int(Decimal(value)))
        elif name == "last_seen_time":
            timestamp = value
            if not isinstance(timestamp, datetime):
                raise ValueError("candidate hash timestamp is invalid")
            value = _iso(timestamp)
        else:
            value = str(value)
        encoded_fields.append(value)
    return hashlib.sha256("\x1f".join(encoded_fields).encode("ascii")).hexdigest()


def _classify_candidate(
    row: StrictV2SCandidateRow,
) -> dict[str, object]:
    current = int(row.current_utxo_sats)
    lifetime = int(row.lifetime_received_sats)
    residual = int(row.residual_gross_90d_sats)
    max_lifetime = int(row.max_same_tx_received_lifetime_sats)
    max_365 = int(row.max_same_tx_received_365d_sats)
    max_90 = int(row.max_same_tx_received_90d_sats)

    retained = (
        max_90 >= 50_000_000_000
        and current >= 1_000_000_000
        and current * 100 >= max_90
    )
    repeated = row.same_tx_receive_ge_500_btc_90d_count >= 2
    sustained = (
        max_90 >= 50_000_000_000
        and row.active_tx_90d_count >= 3
        and row.active_day_90d_count >= 2
        and residual >= 50_000_000_000
    )
    receipt_mask = (
        (1 if retained else 0)
        + (2 if repeated else 0)
        + (4 if sustained else 0)
    )
    strict_receipt = max_90 >= 50_000_000_000 and bool(receipt_mask)
    age_days = (STRICT_V2_S_CUTOFF_TIME - row.last_seen_time).days
    lifetime_supported = (
        lifetime >= 1_000_000_000_000
        and age_days <= 90
        and (
            current >= 1_000_000_000
            or (
                residual >= 50_000_000_000
                and row.active_tx_90d_count >= 3
                and row.active_day_90d_count >= 2
            )
        )
    )
    p0_mask = (
        (1 if current >= 10_000_000_000 else 0)
        + (
            2
            if (
                residual >= 100_000_000_000
                and row.active_tx_90d_count >= 3
                and row.active_day_90d_count >= 2
            )
            else 0
        )
        + (4 if strict_receipt else 0)
        + (8 if lifetime_supported else 0)
    )
    balance_score = _threshold_score(
        current,
        (
            (100_000_000_000, 25),
            (10_000_000_000, 20),
            (1_000_000_000, 12),
            (100_000_000, 5),
        ),
    )
    residual_score = _threshold_score(
        residual,
        (
            (1_000_000_000_000, 20),
            (100_000_000_000, 15),
            (10_000_000_000, 8),
            (1_000_000_000, 3),
        ),
    )
    recent_score = (
        10
        if max_90 >= 50_000_000_000
        else 5
        if max_365 >= 50_000_000_000
        else 0
    )
    repeated_score = (
        12
        if row.same_tx_receive_ge_500_btc_90d_count >= 2
        else 7
        if row.same_tx_receive_ge_500_btc_365d_count >= 2
        else 0
    )
    recency_score = (
        10
        if age_days <= 30
        else 7
        if age_days <= 90
        else 3
        if age_days <= 365
        else 0
    )
    score = (
        balance_score
        + residual_score
        + recent_score
        + repeated_score
        + (10 if retained else 0)
        + (8 if sustained else 0)
        + recency_score
    )
    coarse = (
        p0_mask != 0
        or score >= 15
        or current >= 100_000_000
        or residual >= 1_000_000_000
        or max_365 >= 50_000_000_000
    )
    if not coarse:
        raise ValueError("candidate does not satisfy Strict V2-S coarse union")
    tier = (
        "p0"
        if p0_mask
        else "p1"
        if score >= 25
        else "edge"
        if score >= 15
        else "coarse_other"
    )
    return {
        "v2_chain_score": score,
        "strict_p0_mask": p0_mask,
        "receipt_support_mask": receipt_mask,
        "candidate_tier": tier,
    }


def _threshold_score(
    value: int,
    thresholds: tuple[tuple[int, int], ...],
) -> int:
    for threshold, score in thresholds:
        if value >= threshold:
            return score
    return 0


def _read_execution_receipt(
    request: StrictV2SCandidatePublicationRequest,
) -> dict[str, object]:
    path = request.source_execution_receipt_path
    try:
        metadata = path.stat()
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise CandidatePublicationError(
                "execution receipt mode is invalid"
            )
        encoded = path.read_bytes()
        if hashlib.sha256(encoded).hexdigest() != (
            request.expected_execution_receipt_sha256
        ):
            raise CandidatePublicationError(
                "execution receipt checksum mismatch"
            )
        payload = json.loads(encoded)
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version")
            != STRICT_V2_S_EXECUTION_RECEIPT_VERSION
            or payload.get("status") != "completed"
            or payload.get("destination_table_id")
            != request.destination_table_id
            or payload.get("query_sha256")
            != PINNED_STRICT_V2_S_QUERY_SHA256
            or payload.get("result_schema_sha256")
            != request.expected_result_schema_sha256
            or payload.get("candidate_materialized") is not True
        ):
            raise CandidatePublicationError(
                "execution receipt contract is invalid"
            )
        return payload
    except CandidatePublicationError:
        raise
    except Exception as exc:
        raise CandidatePublicationError(
            "execution receipt is unavailable"
        ) from exc


def _validate_destination_metadata(
    metadata: object,
    *,
    request: StrictV2SCandidatePublicationRequest,
    expected_rows: int,
) -> None:
    if hasattr(metadata, "model_dump"):
        values = metadata.model_dump(mode="python")
    elif isinstance(metadata, Mapping):
        values = metadata
    else:
        raise CandidatePublicationError(
            "destination metadata is malformed"
        )
    if (
        values.get("table_id") != request.destination_table_id
        or values.get("result_schema_sha256")
        != request.expected_result_schema_sha256
        or values.get("row_count") != expected_rows
    ):
        raise CandidatePublicationError(
            "destination metadata does not match publication contract"
        )


def _bounded_record_batches(
    batch: pa.RecordBatch,
    page_size: int,
) -> Iterator[pa.RecordBatch]:
    for offset in range(0, batch.num_rows, page_size):
        yield batch.slice(offset, page_size)


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    table = pa.Table.from_pylist(
        rows,
        schema=CANDIDATE_ARTIFACT_ARROW_SCHEMA,
    )
    pq.write_table(
        table,
        path,
        compression="zstd",
        version="2.6",
        write_statistics=True,
    )
    path.chmod(0o600)


def _file_record(
    path: Path,
    root: Path,
    row_count: int | None,
) -> dict[str, object]:
    return {
        "path": path.relative_to(root).as_posix(),
        "size": path.stat().st_size,
        "sha256": _file_sha256(path),
        "row_count": row_count,
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()


def _write_json_file(path: Path, payload: Mapping[str, object]) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(
            (
                json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
                + "\n"
            ).encode("ascii")
        )
        stream.flush()
        os.fsync(stream.fileno())


def _copy_checksum_pinned_file(
    source: Path,
    target: Path,
    expected_sha256: str,
) -> None:
    encoded = source.read_bytes()
    if hashlib.sha256(encoded).hexdigest() != expected_sha256:
        raise CandidatePublicationError(
            "execution receipt changed during publication"
        )
    descriptor = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return (
        value.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
