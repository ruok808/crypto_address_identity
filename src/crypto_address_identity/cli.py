"""Command-line entry point for the address identity service."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError

from crypto_address_identity import __version__
from crypto_address_identity.audit import (
    build_provider_reliability_panel,
    seed_official_calibration_candidates,
)
from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.consumers.quant_crypto_btc import (
    IdentityEnricher,
    replay_events,
    replay_impact,
)
from crypto_address_identity.consumers.btc_whale_bilateral import replay_bilateral_whale_events
from crypto_address_identity.core.config import Settings
from crypto_address_identity.coverage import (
    CoverageEntitySeedInput,
    CoverageEntitySeedService,
    CoverageSyncService,
)
from crypto_address_identity.evidence import EvidenceInput, EvidenceService, VerifierRegistry
from crypto_address_identity.exports import ResolverExporter
from crypto_address_identity.fetch import FetchService
from crypto_address_identity.proofs.okx_por import (
    OkxBitcoinPorVerifier,
    OkxPorProofError,
    official_okx_evidence_records,
    verified_okx_records,
)
from crypto_address_identity.proofs.bitwise_bitb import (
    BitwiseBitbEvidenceError,
    fetch_bitwise_bitb_snapshot,
    official_bitwise_evidence_records,
)
from crypto_address_identity.providers.zero_x_router import ProviderTokenMissing, ZeroXRouterClient
from crypto_address_identity.providers.zero_x_router import ProviderProfile
from crypto_address_identity.resolver import ResolverService
from crypto_address_identity.storage.raw_payloads import RawPayloadStore
from crypto_address_identity.storage.sqlite import IdentityDatabase
from crypto_address_identity.universe.anchors import CalibrationAnchorReader
from crypto_address_identity.universe.bigquery import (
    BigQueryAddressScaleProbe,
    BigQueryBoundaryError,
    BigQueryCredentialsUnavailable,
    BigQueryDependencyMissing,
    BigQueryProbe,
    GoogleBigQueryBackend,
)
from crypto_address_identity.universe.bitcoin_core import BitcoinCoreProbe
from crypto_address_identity.universe.candidate_statistics import (
    BigQueryCandidateStatisticsProbe,
)
from crypto_address_identity.universe.candidate_statistics_v2 import (
    BigQueryCandidateStatisticsV2Probe,
)
from crypto_address_identity.universe.candidate_execution import (
    CandidateStatisticsExecutionAlreadyAttempted,
    CandidateStatisticsExecutionRequest,
    CandidateStatisticsOneShotExecutor,
    preview_candidate_statistics_execution,
)
from crypto_address_identity.universe.features import (
    BigQueryFeatureMaterializer,
    BigQueryMaterializationRequest,
    FeatureMaterializationError,
)
from crypto_address_identity.universe.models import SourceManifest
from crypto_address_identity.universe.query_plan import BigQueryQueryPlan
from crypto_address_identity.universe.statistics import CandidateStatisticsService
from crypto_address_identity.universe.storage import UniverseIntegrityError, UniverseStore


class CliError(ValueError):
    """Safe user-facing CLI error with no raw exception payload."""

    def __init__(
        self,
        message: str = "invalid CLI input",
        *,
        error_code: str = "invalid_input",
    ) -> None:
        super().__init__(message)
        self.error_code = error_code


class SafeArgumentParser(argparse.ArgumentParser):
    """Convert parser failures into the existing structured JSON boundary."""

    def error(self, message: str) -> None:
        raise CliError("argument parsing failed")


def build_parser() -> argparse.ArgumentParser:
    parser = SafeArgumentParser(prog="cai")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    init_db = commands.add_parser("init-db")
    init_db.set_defaults(handler=_handle_init_db)

    candidates = commands.add_parser("candidates")
    candidate_commands = candidates.add_subparsers(dest="candidate_command", required=True)
    candidate_import = candidate_commands.add_parser("import")
    candidate_import.add_argument("--file", type=Path, required=True)
    candidate_import.add_argument("--dry-run", action="store_true")
    candidate_import.set_defaults(handler=_handle_candidate_import)

    fetch = commands.add_parser("fetch")
    fetch_commands = fetch.add_subparsers(dest="fetch_command", required=True)
    fetch_run = fetch_commands.add_parser("run")
    fetch_run.add_argument("--dry-run", action="store_true")
    fetch_run.add_argument("--limit", type=int, default=100)
    fetch_run.add_argument("--profile", choices=("auto", "discovery"), default="auto")
    fetch_run.add_argument("--source-reference-prefix")
    fetch_run.set_defaults(handler=_handle_fetch_run)

    coverage = commands.add_parser("coverage-sync")
    coverage_commands = coverage.add_subparsers(dest="coverage_command", required=True)
    coverage_seed = coverage_commands.add_parser("seed-entities")
    coverage_seed.add_argument("--file", type=Path, required=True)
    coverage_seed.add_argument("--dry-run", action="store_true")
    coverage_seed.set_defaults(handler=_handle_coverage_seed_entities)
    coverage_run = coverage_commands.add_parser("run")
    coverage_run.add_argument("--dry-run", action="store_true")
    coverage_run.add_argument("--entity-type", action="append", dest="entity_types")
    coverage_run.add_argument("--entity-limit", type=int)
    coverage_run.add_argument("--address-limit", type=int)
    coverage_run.set_defaults(handler=_handle_coverage_sync_run)

    evidence = commands.add_parser("evidence")
    evidence_commands = evidence.add_subparsers(dest="evidence_command", required=True)
    evidence_import = evidence_commands.add_parser("import")
    evidence_import.add_argument("--file", type=Path, required=True)
    evidence_import.add_argument("--dry-run", action="store_true")
    evidence_import.set_defaults(handler=_handle_evidence_import)
    evidence_okx_por = evidence_commands.add_parser("import-okx-btc-por")
    evidence_okx_por.add_argument("--archive", type=Path, required=True)
    evidence_okx_por.add_argument("--source-url", required=True)
    evidence_okx_por.add_argument("--observed-at", required=True)
    evidence_okx_por.add_argument("--limit", type=int, default=50)
    evidence_okx_por.add_argument("--dry-run", action="store_true")
    evidence_okx_por.set_defaults(handler=_handle_evidence_import_okx_btc_por)
    evidence_bitwise_bitb = evidence_commands.add_parser("import-bitwise-bitb")
    evidence_bitwise_bitb.add_argument("--dry-run", action="store_true")
    evidence_bitwise_bitb.set_defaults(handler=_handle_evidence_import_bitwise_bitb)

    resolve = commands.add_parser("resolve")
    resolve_commands = resolve.add_subparsers(dest="resolve_command", required=True)
    resolve_rebuild = resolve_commands.add_parser("rebuild")
    resolve_rebuild.add_argument("--as-of", required=True)
    resolve_rebuild.add_argument("--dry-run", action="store_true")
    resolve_rebuild.set_defaults(handler=_handle_resolve_rebuild)
    resolve_show = resolve_commands.add_parser("show")
    resolve_show.add_argument("--chain", default="bitcoin")
    resolve_show.add_argument("--address", required=True)
    resolve_show.add_argument("--assertion-type", default="entity_control")
    resolve_show.set_defaults(handler=_handle_resolve_show)
    resolve_override = resolve_commands.add_parser("override")
    resolve_override.add_argument("--chain", default="bitcoin")
    resolve_override.add_argument("--address", required=True)
    resolve_override.add_argument("--assertion-type", default="entity_control")
    resolve_override.add_argument("--asserted-value", required=True)
    resolve_override.add_argument("--decision", choices=("select", "reject"), required=True)
    resolve_override.add_argument("--reviewer-ref", required=True)
    resolve_override.add_argument("--reason-ref", required=True)
    resolve_override.add_argument("--reviewed-at", required=True)
    resolve_override.set_defaults(handler=_handle_resolve_override)

    export = commands.add_parser("export")
    export_commands = export.add_subparsers(dest="export_command", required=True)
    export_resolver = export_commands.add_parser("resolver")
    export_resolver.add_argument("--chain", default="bitcoin")
    export_resolver.add_argument("--as-of", required=True)
    export_resolver.add_argument("--dry-run", action="store_true")
    export_resolver.set_defaults(handler=_handle_export_resolver)

    audit = commands.add_parser("audit")
    audit_commands = audit.add_subparsers(dest="audit_command", required=True)
    audit_coverage = audit_commands.add_parser("coverage")
    audit_coverage.add_argument("--chain", default="bitcoin")
    audit_coverage.add_argument("--since", required=True)
    audit_coverage.add_argument("--until", required=True)
    audit_coverage.set_defaults(handler=_handle_audit_coverage)
    audit_provider_panel = audit_commands.add_parser("provider-panel")
    audit_provider_panel.add_argument("--source-reference-prefix", required=True)
    audit_provider_panel.add_argument(
        "--official-evidence-tier",
        action="append",
        choices=("A", "B", "C", "D", "E"),
        dest="official_evidence_tiers",
    )
    audit_provider_panel.add_argument("--official-independence-group")
    audit_provider_panel.set_defaults(handler=_handle_audit_provider_panel)
    audit_seed_provider_panel = audit_commands.add_parser("seed-provider-panel")
    audit_seed_provider_panel.add_argument("--official-independence-group", required=True)
    audit_seed_provider_panel.add_argument("--source-reference", required=True)
    audit_seed_provider_panel.add_argument("--requested-at", required=True)
    audit_seed_provider_panel.add_argument("--priority", type=int, default=70)
    audit_seed_provider_panel.add_argument("--dry-run", action="store_true")
    audit_seed_provider_panel.set_defaults(handler=_handle_audit_seed_provider_panel)

    replay = commands.add_parser("replay")
    replay_commands = replay.add_subparsers(dest="replay_command", required=True)
    replay_btc = replay_commands.add_parser("quant-crypto-btc")
    replay_btc.add_argument("--input", type=Path, required=True)
    replay_btc.add_argument("--snapshot", type=Path, required=True)
    replay_btc.add_argument(
        "--summary-only",
        action="store_true",
        help="emit aggregate non-interference and coverage metrics without event records",
    )
    replay_btc.set_defaults(handler=_handle_replay_btc)
    replay_bilateral = replay_commands.add_parser("btc-whale-bilateral")
    replay_bilateral.add_argument("--input", type=Path, action="append", required=True)
    replay_bilateral.add_argument("--snapshot", type=Path, required=True)
    replay_bilateral.set_defaults(handler=_handle_replay_btc_whale_bilateral)

    universe = commands.add_parser("universe")
    universe_commands = universe.add_subparsers(
        dest="universe_command",
        required=True,
    )
    universe_probe = universe_commands.add_parser("probe")
    universe_probe_commands = universe_probe.add_subparsers(
        dest="universe_probe_command",
        required=True,
    )
    universe_probe_bigquery = universe_probe_commands.add_parser("bigquery")
    bigquery_probe_mode = universe_probe_bigquery.add_mutually_exclusive_group(
        required=True
    )
    bigquery_probe_mode.add_argument("--dry-run", action="store_true")
    bigquery_probe_mode.add_argument("--execute-readonly", action="store_true")
    universe_probe_bigquery.add_argument("--as-of-date", required=True)
    universe_probe_bigquery.add_argument(
        "--maximum-bytes-billed",
        type=int,
        default=0,
    )
    universe_probe_bigquery.set_defaults(handler=_handle_universe_probe_bigquery)

    universe_probe_address_scale = universe_probe_commands.add_parser(
        "bigquery-address-scale"
    )
    address_scale_probe_mode = (
        universe_probe_address_scale.add_mutually_exclusive_group(required=True)
    )
    address_scale_probe_mode.add_argument("--dry-run", action="store_true")
    address_scale_probe_mode.add_argument(
        "--execute-readonly",
        action="store_true",
    )
    universe_probe_address_scale.add_argument("--as-of-date", required=True)
    universe_probe_address_scale.add_argument(
        "--sandbox-budget-bytes",
        type=int,
        default=0,
    )
    universe_probe_address_scale.set_defaults(
        handler=_handle_universe_probe_bigquery_address_scale
    )

    universe_probe_candidate_statistics = universe_probe_commands.add_parser(
        "bigquery-candidate-statistics"
    )
    candidate_statistics_mode = (
        universe_probe_candidate_statistics.add_mutually_exclusive_group(
            required=True
        )
    )
    candidate_statistics_mode.add_argument("--dry-run", action="store_true")
    candidate_statistics_mode.add_argument(
        "--execute-readonly",
        action="store_true",
    )
    universe_probe_candidate_statistics.add_argument(
        "--as-of-date", required=True
    )
    universe_probe_candidate_statistics.add_argument(
        "--cutoff-height", type=int, required=True
    )
    universe_probe_candidate_statistics.add_argument(
        "--expected-query-sha256"
    )
    universe_probe_candidate_statistics.add_argument(
        "--sandbox-budget-bytes",
        type=int,
        default=0,
    )
    universe_probe_candidate_statistics.add_argument(
        "--reserve-bytes",
        type=int,
        default=250_000_000_000,
    )
    universe_probe_candidate_statistics.set_defaults(
        handler=_handle_universe_probe_bigquery_candidate_statistics
    )

    universe_probe_candidate_statistics_v2 = universe_probe_commands.add_parser(
        "bigquery-candidate-statistics-v2"
    )
    candidate_statistics_v2_mode = (
        universe_probe_candidate_statistics_v2.add_mutually_exclusive_group(
            required=True
        )
    )
    candidate_statistics_v2_mode.add_argument("--dry-run", action="store_true")
    candidate_statistics_v2_mode.add_argument(
        "--live-dry-run",
        action="store_true",
    )
    universe_probe_candidate_statistics_v2.add_argument(
        "--as-of-date",
        required=True,
    )
    universe_probe_candidate_statistics_v2.add_argument(
        "--cutoff-height",
        type=int,
        required=True,
    )
    universe_probe_candidate_statistics_v2.add_argument(
        "--expected-query-sha256"
    )
    universe_probe_candidate_statistics_v2.add_argument(
        "--sandbox-budget-bytes",
        type=int,
        default=0,
    )
    universe_probe_candidate_statistics_v2.add_argument(
        "--reserve-bytes",
        type=int,
        default=250_000_000_000,
    )
    universe_probe_candidate_statistics_v2.set_defaults(
        handler=_handle_universe_probe_bigquery_candidate_statistics_v2
    )

    universe_execute = universe_commands.add_parser("execute")
    universe_execute_commands = universe_execute.add_subparsers(
        dest="universe_execute_command",
        required=True,
    )
    universe_execute_candidate_statistics = universe_execute_commands.add_parser(
        "bigquery-candidate-statistics"
    )
    candidate_execution_mode = (
        universe_execute_candidate_statistics.add_mutually_exclusive_group(
            required=True
        )
    )
    candidate_execution_mode.add_argument("--dry-run", action="store_true")
    candidate_execution_mode.add_argument("--execute-once", action="store_true")
    universe_execute_candidate_statistics.add_argument(
        "--authorization-id",
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--as-of-date",
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--cutoff-height",
        type=int,
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--expected-query-sha256",
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--expected-schema-sha256",
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--expected-source-address-count",
        type=int,
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--maximum-bytes-billed",
        type=int,
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--sandbox-budget-bytes",
        type=int,
        required=True,
    )
    universe_execute_candidate_statistics.add_argument(
        "--reserve-bytes",
        type=int,
        required=True,
    )
    universe_execute_candidate_statistics.set_defaults(
        handler=_handle_universe_execute_bigquery_candidate_statistics
    )

    universe_probe_bitcoin = universe_probe_commands.add_parser("bitcoin-core")
    bitcoin_probe_mode = universe_probe_bitcoin.add_mutually_exclusive_group(
        required=True
    )
    bitcoin_probe_mode.add_argument("--dry-run", action="store_true")
    bitcoin_probe_mode.add_argument("--execute-readonly", action="store_true")
    universe_probe_bitcoin.set_defaults(handler=_handle_universe_probe_bitcoin)

    universe_build = universe_commands.add_parser("build")
    universe_build_commands = universe_build.add_subparsers(
        dest="universe_build_command",
        required=True,
    )
    universe_build_bigquery = universe_build_commands.add_parser("bigquery")
    bigquery_build_mode = universe_build_bigquery.add_mutually_exclusive_group(
        required=True
    )
    bigquery_build_mode.add_argument("--dry-run", action="store_true")
    bigquery_build_mode.add_argument("--execute-chain-read", action="store_true")
    universe_build_bigquery.add_argument("--campaign-id", required=True)
    universe_build_bigquery.add_argument(
        "--cutoff-height",
        type=int,
        required=True,
    )
    universe_build_bigquery.add_argument("--cutoff-time", required=True)
    universe_build_bigquery.add_argument(
        "--maximum-bytes-billed",
        type=int,
        default=0,
    )
    universe_build_bigquery.set_defaults(handler=_handle_universe_build_bigquery)

    universe_candidates = universe_commands.add_parser("candidates")
    universe_candidates.add_argument("--campaign-id", required=True)
    universe_candidates.add_argument("--dry-run", action="store_true", required=True)
    universe_candidates.add_argument(
        "--runtime-minutes",
        type=int,
        default=480,
    )
    universe_candidates.add_argument(
        "--requests-per-minute",
        type=int,
        default=25,
    )
    universe_candidates.add_argument(
        "--estimated-points-per-address",
        type=int,
    )
    universe_candidates.add_argument(
        "--discovery-point-budget",
        type=int,
        default=0,
    )
    universe_candidates.set_defaults(handler=_handle_universe_candidates)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        arguments = parser.parse_args(argv)
        handler: Callable[[argparse.Namespace], dict[str, Any]] = arguments.handler
        _emit(handler(arguments))
        return 0
    except ProviderTokenMissing:
        _emit({"status": "error", "error_code": "provider_token_missing"})
        return 2
    except CliError as exc:
        _emit({"status": "error", "error_code": exc.error_code})
        return 2
    except BigQueryDependencyMissing:
        _emit({"status": "error", "error_code": "bigquery_dependency_missing"})
        return 2
    except BigQueryCredentialsUnavailable:
        _emit({"status": "error", "error_code": "bigquery_credentials_unavailable"})
        return 2
    except BigQueryBoundaryError:
        _emit({"status": "error", "error_code": "bigquery_schema_blocked"})
        return 2
    except FeatureMaterializationError:
        _emit({"status": "error", "error_code": "universe_integrity_error"})
        return 2
    except UniverseIntegrityError:
        _emit({"status": "error", "error_code": "universe_integrity_error"})
        return 2
    except (ValidationError, ValueError):
        _emit({"status": "error", "error_code": "invalid_input"})
        return 2
    except Exception:
        _emit({"status": "error", "error_code": "internal_error"})
        return 1


def _handle_init_db(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    IdentityDatabase(settings.database_path).migrate()
    return {"status": "ok", "database_initialized": True, "config": settings.safe_summary()}


def _handle_candidate_import(arguments: argparse.Namespace) -> dict[str, Any]:
    records = [CandidateInput.model_validate(value) for value in _read_ndjson(arguments.file)]
    if arguments.dry_run:
        return {"status": "dry_run", "records": len(records)}
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    result = CandidateService(database).import_candidates(records)
    return {
        "status": "ok",
        "records": len(records),
        "imported_count": result.imported_count,
        "candidate_request_ids": list(result.candidate_request_ids),
    }


def _handle_fetch_run(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    if not arguments.dry_run:
        database.migrate()
        if settings.provider_token_value() is None:
            raise ProviderTokenMissing()
    provider = ZeroXRouterClient(settings)
    try:
        service = FetchService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
            evidence=EvidenceService(database, VerifierRegistry()),
        )
        result = asdict(
            service.run(
                dry_run=arguments.dry_run,
                limit=arguments.limit,
                profile_override=(
                    ProviderProfile.DISCOVERY if arguments.profile == "discovery" else None
                ),
                source_reference_prefix=arguments.source_reference_prefix,
            )
        )
        result["profile_override"] = arguments.profile
        return result
    finally:
        provider.close()


def _handle_coverage_seed_entities(arguments: argparse.Namespace) -> dict[str, Any]:
    records = [CoverageEntitySeedInput.model_validate(value) for value in _read_ndjson(arguments.file)]
    if arguments.dry_run:
        return {"status": "dry_run", "records": len(records)}
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    result = CoverageEntitySeedService(database).import_seeds(records)
    return {"status": "ok", "records": len(records), **asdict(result)}


def _handle_coverage_sync_run(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    if not arguments.dry_run:
        database.migrate()
        if settings.provider_token_value() is None:
            raise ProviderTokenMissing()
    provider = ZeroXRouterClient(settings)
    try:
        result = CoverageSyncService(
            database=database,
            settings=settings,
            provider=provider,
            raw_payloads=RawPayloadStore(database, settings.raw_payload_root),
            evidence=EvidenceService(database, VerifierRegistry()),
        ).run(
            dry_run=arguments.dry_run,
            entity_types=tuple(arguments.entity_types or ("exchange", "fund")),
            entity_limit=arguments.entity_limit,
            address_limit=arguments.address_limit,
        )
        return asdict(result)
    finally:
        provider.close()


def _handle_evidence_import(arguments: argparse.Namespace) -> dict[str, Any]:
    records = [EvidenceInput.model_validate(value) for value in _read_ndjson(arguments.file)]
    if arguments.dry_run:
        return {"status": "dry_run", "records": len(records)}
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    result = EvidenceService(database, VerifierRegistry()).import_records(records)
    return {"status": "ok", "records": len(records), **asdict(result)}


def _handle_evidence_import_okx_btc_por(arguments: argparse.Namespace) -> dict[str, Any]:
    if arguments.limit < 1:
        raise CliError("limit must be positive")
    try:
        archive_payload = arguments.archive.read_bytes()
    except OSError as exc:
        raise CliError("Unable to read official PoR archive") from exc
    if len(archive_payload) > 100 * 1024 * 1024:
        raise CliError("Official PoR archive exceeds the configured input limit")
    try:
        observed_at = _parse_utc_datetime(arguments.observed_at)
        verified, summary = verified_okx_records(archive_payload, limit=arguments.limit)
    except OkxPorProofError as exc:
        raise CliError("Official PoR archive is invalid") from exc

    artifact_sha256 = hashlib.sha256(archive_payload).hexdigest()
    output = {
        "status": "dry_run" if arguments.dry_run else "ok",
        "source": "okx_btc_por",
        "artifact_sha256": artifact_sha256,
        "parsed_btc_multisig_rows": summary.parsed_btc_multisig_rows,
        "verification_candidate_rows": summary.verification_candidate_rows,
        "verified_rows": summary.verified_rows,
        "invalid_rows": summary.invalid_rows,
        "selected_rows": summary.selected_rows,
        "written_paths": [],
    }
    if arguments.dry_run:
        return output
    if not verified:
        raise CliError("Official PoR archive has no verified BTC multisig proofs")
    try:
        evidence_records = official_okx_evidence_records(
            verified,
            source_url=arguments.source_url,
            artifact_sha256=artifact_sha256,
            observed_at=observed_at,
        )
    except ValidationError as exc:
        raise CliError("Official PoR source metadata is invalid") from exc

    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    stored = RawPayloadStore(database, settings.raw_payload_root).persist(archive_payload)
    if stored.payload_sha256 != artifact_sha256:
        raise CliError("Official PoR artifact integrity check failed")
    verifier = OkxBitcoinPorVerifier(
        artifact_sha256=stored.payload_sha256,
        verified_addresses=[record.address for record in verified],
    )
    result = EvidenceService(database, VerifierRegistry([verifier])).import_records(evidence_records)
    output.update(
        {
            "inserted_count": result.inserted_count,
            "duplicate_count": result.duplicate_count,
            "raw_payload_status": RawPayloadStore(database, settings.raw_payload_root)
            .verify(stored.payload_sha256)
            .status,
            "written_paths": [stored.relative_path],
        }
    )
    return output


def _handle_evidence_import_bitwise_bitb(arguments: argparse.Namespace) -> dict[str, Any]:
    try:
        with httpx.Client(timeout=30.0, follow_redirects=False, trust_env=False) as client:
            snapshot = fetch_bitwise_bitb_snapshot(client)
    except BitwiseBitbEvidenceError as exc:
        raise CliError("BITB issuer address publication is unavailable or malformed") from exc

    safe_payload = snapshot.safe_payload()
    artifact_sha256 = hashlib.sha256(safe_payload).hexdigest()
    evidence_records = official_bitwise_evidence_records(snapshot, artifact_sha256=artifact_sha256)
    output = {
        "status": "dry_run" if arguments.dry_run else "ok",
        "source": "bitwise_bitb_public_wallets",
        "address_count": len(snapshot.addresses),
        "source_page_sha256": snapshot.source_page_sha256,
        "reported_updated_at": snapshot.reported_updated_at,
        "artifact_sha256": artifact_sha256,
        "written_paths": [],
    }
    if arguments.dry_run:
        return output

    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    raw_payloads = RawPayloadStore(database, settings.raw_payload_root)
    stored = raw_payloads.persist(safe_payload)
    if stored.payload_sha256 != artifact_sha256:
        raise CliError("BITB sanitized snapshot integrity check failed")
    result = EvidenceService(database, VerifierRegistry()).import_records(evidence_records)
    output.update(
        {
            "inserted_count": result.inserted_count,
            "duplicate_count": result.duplicate_count,
            "raw_payload_status": raw_payloads.verify(stored.payload_sha256).status,
            "written_paths": [stored.relative_path],
        }
    )
    return output


def _handle_resolve_rebuild(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    if arguments.dry_run:
        return {"status": "dry_run", "as_of": arguments.as_of}
    database.migrate()
    result = ResolverService(database).rebuild(as_of=arguments.as_of)
    return {"status": "ok", **asdict(result)}


def _handle_resolve_show(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    result = ResolverService(IdentityDatabase(settings.database_path)).show(
        arguments.chain, arguments.address, assertion_type=arguments.assertion_type
    )
    return {"status": "ok", **asdict(result)}


def _handle_resolve_override(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    override_id = ResolverService(database).record_local_override(
        chain_key=arguments.chain,
        address=arguments.address,
        assertion_type=arguments.assertion_type,
        asserted_value=arguments.asserted_value,
        decision=arguments.decision,
        reviewer_ref=arguments.reviewer_ref,
        reason_ref=arguments.reason_ref,
        reviewed_at=arguments.reviewed_at,
    )
    return {"status": "ok", "override_id": override_id, "requires_rebuild": True}


def _handle_export_resolver(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    result = ResolverExporter(IdentityDatabase(settings.database_path), settings.export_root).export(
        chain_key=arguments.chain, as_of=arguments.as_of, dry_run=arguments.dry_run
    )
    output = asdict(result)
    output["directory"] = str(result.directory)
    output["status"] = "dry_run" if arguments.dry_run else "ok"
    return output


def _handle_audit_coverage(arguments: argparse.Namespace) -> dict[str, Any]:
    if arguments.chain != "bitcoin":
        raise CliError("BTC-first only")
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    with database.read_connection() as connection:
        observations = connection.execute(
            """
            SELECT outcome, COUNT(*) AS count FROM source_observation
            WHERE chain_key = ? AND completed_at >= ? AND completed_at <= ?
            GROUP BY outcome ORDER BY outcome
            """,
            (arguments.chain, arguments.since, arguments.until),
        ).fetchall()
        tiers = connection.execute(
            """
            SELECT evidence_tier, COUNT(*) AS count FROM identity_evidence
            WHERE observed_at >= ? AND observed_at <= ?
            GROUP BY evidence_tier ORDER BY evidence_tier
            """,
            (arguments.since, arguments.until),
        ).fetchall()
        conflicts = connection.execute(
            "SELECT COUNT(*) FROM conflict_set WHERE status = 'active'"
        ).fetchone()[0]
    return {
        "status": "ok",
        "chain": arguments.chain,
        "observation_outcomes": {row["outcome"]: row["count"] for row in observations},
        "evidence_tiers": {row["evidence_tier"]: row["count"] for row in tiers},
        "active_conflicts": conflicts,
    }


def _handle_audit_provider_panel(arguments: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    return build_provider_reliability_panel(
        IdentityDatabase(settings.database_path),
        source_reference_prefix=arguments.source_reference_prefix,
        official_evidence_tiers=tuple(arguments.official_evidence_tiers or ("A",)),
        official_independence_group=arguments.official_independence_group,
    )


def _handle_audit_seed_provider_panel(arguments: argparse.Namespace) -> dict[str, Any]:
    requested_at = _parse_utc_datetime(arguments.requested_at)
    settings = Settings()
    database = IdentityDatabase(settings.database_path)
    database.migrate()
    result = seed_official_calibration_candidates(
        database,
        independence_group=arguments.official_independence_group,
        source_reference=arguments.source_reference,
        requested_at=requested_at,
        priority=arguments.priority,
        dry_run=arguments.dry_run,
    )
    return {
        "status": "dry_run" if arguments.dry_run else "ok",
        **asdict(result),
    }


def _handle_replay_btc(arguments: argparse.Namespace) -> dict[str, Any]:
    events = _read_ndjson(arguments.input)
    enricher = IdentityEnricher.from_snapshot_directory(arguments.snapshot)
    result = replay_events(events, enricher)
    impact = replay_impact(events, enricher)
    output = {
        "status": "ok",
        "input_records": len(result.events),
        "events": impact.events,
        "changed_business_fields": result.changed_business_fields,
        "impact": asdict(impact),
    }
    if not arguments.summary_only:
        output["enriched_events"] = list(result.events)
    return output


def _handle_replay_btc_whale_bilateral(arguments: argparse.Namespace) -> dict[str, Any]:
    events = [event for path in arguments.input for event in _read_ndjson(path)]
    impact = replay_bilateral_whale_events(
        events, IdentityEnricher.from_snapshot_directory(arguments.snapshot)
    )
    return {
        "status": "ok",
        "input_records": len(events),
        "bilateral_impact": asdict(impact),
    }


def _handle_universe_probe_bigquery(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    as_of_date = _parse_date(arguments.as_of_date)
    plan = BigQueryQueryPlan.load(settings.bigquery_dataset)
    if arguments.dry_run:
        return {
            "status": "dry_run",
            "source_kind": "bigquery",
            "as_of_date": as_of_date.isoformat(),
            "query_sha256": plan.address_features_sha256,
            "checkpoint_query_sha256": plan.source_checkpoint_sha256,
            "network_requests": 0,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    if arguments.maximum_bytes_billed <= 0:
        raise CliError("positive BigQuery byte cap required")

    cutoff_time = datetime.combine(as_of_date, time.max, tzinfo=UTC)
    result = BigQueryProbe(
        backend=_make_bigquery_backend(settings),
        dataset=settings.bigquery_dataset,
        max_source_age=timedelta(hours=settings.universe_max_source_age_hours),
    ).run(
        as_of_date=as_of_date,
        cutoff_height=None,
        cutoff_time=cutoff_time,
        maximum_bytes_billed=0,
        execute_checkpoint=True,
        checkpoint_maximum_bytes_billed=arguments.maximum_bytes_billed,
    )
    return {
        **result.model_dump(mode="json"),
        "query_sha256": plan.address_features_sha256,
        "checkpoint_query_sha256": plan.source_checkpoint_sha256,
        "provider_requests": 0,
        "provider_points": 0,
        "written_paths": [],
    }


def _handle_universe_probe_bigquery_address_scale(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    as_of_date = _parse_date(arguments.as_of_date)
    plan = BigQueryQueryPlan.load(settings.bigquery_dataset)
    if arguments.dry_run:
        return {
            "status": "dry_run",
            "source_kind": "bigquery",
            "query_kind": "btc_address_scale",
            "as_of_date": as_of_date.isoformat(),
            "query_sha256": plan.address_scale_sha256,
            "network_requests": 0,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    if arguments.sandbox_budget_bytes <= 0:
        raise CliError("positive BigQuery Sandbox budget required")

    result = BigQueryAddressScaleProbe(
        backend=_make_bigquery_backend(settings),
        dataset=settings.bigquery_dataset,
        max_source_age=timedelta(hours=settings.universe_max_source_age_hours),
    ).run(
        cutoff_height=9_223_372_036_854_775_807,
        cutoff_time=datetime.combine(as_of_date, time.max, tzinfo=UTC),
        sandbox_budget_bytes=arguments.sandbox_budget_bytes,
    )
    return {
        **result.model_dump(mode="json"),
        "as_of_date": as_of_date.isoformat(),
        "network_requests": 2,
        "provider_requests": 0,
        "provider_points": 0,
        "written_paths": [],
    }


def _handle_universe_probe_bigquery_candidate_statistics(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    as_of_date = _parse_date(arguments.as_of_date)
    if arguments.cutoff_height < 0:
        raise CliError("cutoff height must be non-negative")
    cutoff_time = datetime.combine(as_of_date, time.max, tzinfo=UTC)
    plan = BigQueryQueryPlan.load(settings.bigquery_dataset)
    if arguments.dry_run:
        return {
            "status": "dry_run",
            "source_kind": "bigquery",
            "query_kind": "btc_candidate_statistics",
            "read_only": True,
            "as_of_date": as_of_date.isoformat(),
            "cutoff_height": arguments.cutoff_height,
            "cutoff_time": cutoff_time.isoformat().replace("+00:00", "Z"),
            "query_sha256": plan.candidate_statistics_sha256,
            "network_requests": 0,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    if not arguments.expected_query_sha256:
        raise CliError("expected candidate query SHA-256 required")
    if arguments.sandbox_budget_bytes <= 0:
        raise CliError("positive BigQuery Sandbox budget required")
    if (
        arguments.reserve_bytes < 0
        or arguments.reserve_bytes >= arguments.sandbox_budget_bytes
    ):
        raise CliError("BigQuery reserve must be below Sandbox budget")

    result = BigQueryCandidateStatisticsProbe(
        backend=_make_bigquery_backend(settings),
        dataset=settings.bigquery_dataset,
        max_source_age=timedelta(
            hours=settings.universe_max_source_age_hours
        ),
    ).run(
        cutoff_height=arguments.cutoff_height,
        cutoff_time=cutoff_time,
        expected_query_sha256=arguments.expected_query_sha256,
        sandbox_budget_bytes=arguments.sandbox_budget_bytes,
        reserve_bytes=arguments.reserve_bytes,
    )
    return {
        **result.model_dump(mode="json"),
        "as_of_date": as_of_date.isoformat(),
        "cutoff_height": arguments.cutoff_height,
        "cutoff_time": cutoff_time.isoformat().replace("+00:00", "Z"),
        "provider_requests": 0,
        "provider_points": 0,
        "written_paths": [],
    }


def _handle_universe_probe_bigquery_candidate_statistics_v2(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    as_of_date = _parse_date(arguments.as_of_date)
    if arguments.cutoff_height < 0:
        raise CliError("cutoff height must be non-negative")
    cutoff_time = datetime.combine(as_of_date, time.max, tzinfo=UTC)
    plan = BigQueryQueryPlan.load(settings.bigquery_dataset)
    if arguments.dry_run:
        return {
            "status": "dry_run",
            "source_kind": "bigquery",
            "query_kind": "btc_candidate_statistics_v2",
            "policy_version": "btc_importance_v2",
            "read_only": True,
            "as_of_date": as_of_date.isoformat(),
            "cutoff_height": arguments.cutoff_height,
            "cutoff_time": cutoff_time.isoformat().replace("+00:00", "Z"),
            "query_sha256": plan.candidate_statistics_v2_sha256,
            "network_requests": 0,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    if not arguments.expected_query_sha256:
        raise CliError("expected v2 candidate query SHA-256 required")
    if arguments.sandbox_budget_bytes <= 0:
        raise CliError("positive BigQuery Sandbox budget required")
    if (
        arguments.reserve_bytes < 0
        or arguments.reserve_bytes >= arguments.sandbox_budget_bytes
    ):
        raise CliError("BigQuery reserve must be below Sandbox budget")

    result = BigQueryCandidateStatisticsV2Probe(
        backend=_make_bigquery_backend(settings),
        dataset=settings.bigquery_dataset,
        max_source_age=timedelta(
            hours=settings.universe_max_source_age_hours
        ),
    ).run(
        cutoff_height=arguments.cutoff_height,
        cutoff_time=cutoff_time,
        expected_query_sha256=arguments.expected_query_sha256,
        sandbox_budget_bytes=arguments.sandbox_budget_bytes,
        reserve_bytes=arguments.reserve_bytes,
    )
    return {
        **result.model_dump(mode="json"),
        "as_of_date": as_of_date.isoformat(),
        "cutoff_height": arguments.cutoff_height,
        "cutoff_time": cutoff_time.isoformat().replace("+00:00", "Z"),
        "provider_requests": 0,
        "provider_points": 0,
        "written_paths": [],
    }


def _handle_universe_execute_bigquery_candidate_statistics(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    request = CandidateStatisticsExecutionRequest(
        authorization_id=arguments.authorization_id,
        as_of_date=_parse_date(arguments.as_of_date),
        cutoff_height=arguments.cutoff_height,
        expected_query_sha256=arguments.expected_query_sha256,
        expected_schema_sha256=arguments.expected_schema_sha256,
        expected_source_standard_address_count=(
            arguments.expected_source_address_count
        ),
        maximum_bytes_billed=arguments.maximum_bytes_billed,
        sandbox_budget_bytes=arguments.sandbox_budget_bytes,
        reserve_bytes=arguments.reserve_bytes,
    )
    receipt_root = settings.universe_root / "executions"
    if arguments.dry_run:
        outcome = preview_candidate_statistics_execution(
            request,
            dataset=settings.bigquery_dataset,
            receipt_root=receipt_root,
        )
    else:
        try:
            outcome = CandidateStatisticsOneShotExecutor(
                backend=_make_bigquery_backend(settings),
                dataset=settings.bigquery_dataset,
                receipt_root=receipt_root,
                max_source_age=timedelta(
                    hours=settings.universe_max_source_age_hours
                ),
            ).run(request)
        except CandidateStatisticsExecutionAlreadyAttempted as exc:
            raise CliError(
                "candidate execution authorization was already attempted",
                error_code="candidate_execution_already_attempted",
            ) from exc
    return outcome.model_dump(mode="json")


def _handle_universe_probe_bitcoin(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    if arguments.dry_run:
        return {
            "status": "dry_run",
            "source_kind": "bitcoin_core",
            "network_requests": 0,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    result = _make_bitcoin_core_probe(Settings()).run()
    return {
        **result.model_dump(mode="json"),
        "provider_requests": 0,
        "provider_points": 0,
        "written_paths": [],
    }


def _handle_universe_build_bigquery(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    cutoff_time = _parse_utc_datetime(arguments.cutoff_time).astimezone(UTC)
    if arguments.cutoff_height < 0:
        raise CliError("cutoff height must be non-negative")
    plan = BigQueryQueryPlan.load(settings.bigquery_dataset)
    if arguments.dry_run:
        _validate_campaign_arguments(
            campaign_id=arguments.campaign_id,
            cutoff_height=arguments.cutoff_height,
            cutoff_time=cutoff_time,
            query_sha256=plan.address_features_sha256,
        )
        return {
            "status": "dry_run",
            "campaign_id": arguments.campaign_id,
            "cutoff_height": arguments.cutoff_height,
            "cutoff_time": cutoff_time.isoformat().replace("+00:00", "Z"),
            "query_sha256": plan.address_features_sha256,
            "dry_run_bytes": None,
            "network_requests": 0,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    if arguments.maximum_bytes_billed <= 0:
        raise CliError("positive BigQuery byte cap required")

    backend = _make_bigquery_backend(settings)
    probe = BigQueryProbe(
        backend=backend,
        dataset=settings.bigquery_dataset,
        max_source_age=timedelta(hours=settings.universe_max_source_age_hours),
    ).run(
        as_of_date=cutoff_time.date(),
        cutoff_height=arguments.cutoff_height,
        cutoff_time=cutoff_time,
        maximum_bytes_billed=arguments.maximum_bytes_billed,
        execute_checkpoint=True,
        checkpoint_maximum_bytes_billed=arguments.maximum_bytes_billed,
    )
    if probe.status != "accepted":
        return {
            **probe.model_dump(mode="json"),
            "campaign_id": arguments.campaign_id,
            "query_sha256": plan.address_features_sha256,
            "provider_requests": 0,
            "provider_points": 0,
            "written_paths": [],
        }
    if (
        probe.schema_sha256 is None
        or probe.finalized_hash is None
        or probe.finalized_height != arguments.cutoff_height
    ):
        raise CliError(
            "BigQuery checkpoint is incomplete",
            error_code="bigquery_schema_blocked",
        )
    source_manifest = SourceManifest(
        campaign_id=arguments.campaign_id,
        source_kind="bigquery",
        source_revision=(
            f"{settings.bigquery_dataset}@{arguments.cutoff_height}:"
            f"{probe.finalized_hash}"
        ),
        cutoff_height=arguments.cutoff_height,
        cutoff_hash=probe.finalized_hash,
        cutoff_time=cutoff_time,
        schema_sha256=probe.schema_sha256,
        query_sha256=plan.address_features_sha256,
        source_capabilities=probe.capabilities,
        script_completeness=probe.script_completeness,
    )
    calibration_snapshot = (
        CalibrationAnchorReader(settings.database_path).read(as_of=cutoff_time)
        if settings.database_path.is_file()
        else None
    )
    result = _make_bigquery_materializer(settings, backend).run(
        request=BigQueryMaterializationRequest(
            source_manifest=source_manifest,
            dataset=settings.bigquery_dataset,
            maximum_bytes_billed=arguments.maximum_bytes_billed,
        ),
        calibration_snapshot=calibration_snapshot,
    )
    return result.model_dump(mode="json")


def _handle_universe_candidates(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    settings = Settings()
    store = UniverseStore(settings.universe_root)
    try:
        campaign = store.load(arguments.campaign_id)
    except UniverseIntegrityError as exc:
        raise CliError(
            "campaign not found",
            error_code="campaign_not_found",
        ) from exc
    try:
        result = CandidateStatisticsService(campaign).dry_run(
            runtime_minutes=arguments.runtime_minutes,
            requests_per_minute=arguments.requests_per_minute,
            estimated_points_per_address=arguments.estimated_points_per_address,
            discovery_point_budget=arguments.discovery_point_budget,
        )
    except UniverseIntegrityError as exc:
        raise CliError(
            "candidate statistics blocked",
            error_code="candidate_stats_blocked",
        ) from exc
    return result.model_dump(mode="json")


def _make_bigquery_backend(settings: Settings) -> GoogleBigQueryBackend:
    if not settings.bigquery_billing_project:
        raise BigQueryCredentialsUnavailable(
            "BigQuery billing project is unavailable"
        )
    return GoogleBigQueryBackend(
        billing_project=settings.bigquery_billing_project,
        location=settings.bigquery_location,
    )


def _make_bitcoin_core_probe(settings: Settings) -> BitcoinCoreProbe:
    return BitcoinCoreProbe(settings)


def _make_bigquery_materializer(
    settings: Settings,
    backend: Any,
) -> BigQueryFeatureMaterializer:
    return BigQueryFeatureMaterializer(
        backend=backend,
        store=UniverseStore(settings.universe_root),
    )


def _validate_campaign_arguments(
    *,
    campaign_id: str,
    cutoff_height: int,
    cutoff_time: datetime,
    query_sha256: str,
) -> None:
    SourceManifest(
        campaign_id=campaign_id,
        source_kind="bigquery",
        source_revision="offline-plan",
        cutoff_height=cutoff_height,
        cutoff_hash="00" * 32,
        cutoff_time=cutoff_time,
        schema_sha256="00" * 32,
        query_sha256=query_sha256,
        source_capabilities=("offline_plan",),
        script_completeness=True,
    )


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise CliError("Unable to read input file") from exc
    values: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CliError("Input is not valid NDJSON") from exc
        if not isinstance(value, dict):
            raise CliError("NDJSON records must be objects")
        values.append(value)
    return values


def _parse_utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CliError("Timestamp must be ISO-8601 UTC") from exc
    if parsed.tzinfo is None:
        raise CliError("Timestamp must be timezone-aware")
    return parsed


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise CliError("Date must be ISO-8601 YYYY-MM-DD") from exc


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True, default=str))
