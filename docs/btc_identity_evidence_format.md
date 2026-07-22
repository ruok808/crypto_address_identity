# BTC Identity Evidence Import Format

Evidence import uses UTF-8 NDJSON, one JSON object per assertion. The importer
creates an immutable `import` observation before inserting a deduplicated
evidence row.

## Required Fields

```json
{
  "chain_key": "bitcoin",
  "address": "<bitcoin-mainnet-address>",
  "assertion_type": "entity_control",
  "candidate_entity_id": "<stable-local-or-source-entity-id>",
  "candidate_entity_name": "<display-name>",
  "source_authority": "official",
  "evidence_tier": "B",
  "verification_method": "published-list",
  "source_url": "https://public.example.invalid/address-list",
  "artifact_sha256": "<64-lowercase-hex-characters>",
  "license_ref": "<license-or-terms-reference>",
  "independence_group": "<source-family>",
  "observed_at": "2026-07-22T00:00:00Z",
  "effective_from": "2026-07-01T00:00:00Z",
  "evidence_status": "valid",
  "imported_by": "<operator-or-importer-id>"
}
```

`assertion_type` is one of `entity_control`, `address_label`, `wallet_role`,
`address_kind`, or `relationship`. Keep entity control, an address label, and a
wallet role in separate rows. A free-text provider tag such as `hot` or
`deposit` is an address-label candidate, not a wallet role in the BTC-first
phase.

## Tier A

Tier A needs `verification_result: "valid"` in input and a named local proof
verifier that independently returns `valid`. Invalid or unsupported proof
results are rejected as Tier A. They may be imported at a lower appropriate tier
only after source review.

## Source Safety

`source_url` must be a public HTTPS URL without credentials, provider token,
API key, authorization material, or signed query. The input must not contain a
private key, signed request, request header, or provider token. Use an artifact
hash rather than embedding the artifact body in an evidence record.

## Evidence Semantics

0xRouter/Arkham observations use Tier C, `commercial_provider`,
`api-observation`, and a stable `arkham_0xrouter` independence group. They are
discovery evidence only. Conflicting values remain separate, auditable claims;
they do not overwrite local or official evidence.
