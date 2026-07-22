"""Command-line entry point for the address identity service."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError

from crypto_address_identity import __version__
from crypto_address_identity.audit import build_provider_reliability_panel
from crypto_address_identity.candidates import CandidateInput, CandidateService
from crypto_address_identity.consumers.quant_crypto_btc import IdentityEnricher, replay_events
from crypto_address_identity.core.config import Settings
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


class CliError(ValueError):
    """Safe user-facing CLI error with no raw exception payload."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cai")
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
    fetch_run.set_defaults(handler=_handle_fetch_run)

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
    audit_provider_panel.set_defaults(handler=_handle_audit_provider_panel)

    replay = commands.add_parser("replay")
    replay_commands = replay.add_subparsers(dest="replay_command", required=True)
    replay_btc = replay_commands.add_parser("quant-crypto-btc")
    replay_btc.add_argument("--input", type=Path, required=True)
    replay_btc.add_argument("--snapshot", type=Path, required=True)
    replay_btc.set_defaults(handler=_handle_replay_btc)
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
    except (CliError, ValidationError, ValueError):
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
            )
        )
        result["profile_override"] = arguments.profile
        return result
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
    )


def _handle_replay_btc(arguments: argparse.Namespace) -> dict[str, Any]:
    events = _read_ndjson(arguments.input)
    result = replay_events(events, IdentityEnricher.from_snapshot_directory(arguments.snapshot))
    return {
        "status": "ok",
        "events": len(result.events),
        "changed_business_fields": result.changed_business_fields,
        "enriched_events": list(result.events),
    }


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


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True, default=str))
