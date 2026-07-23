# crypto_address_identity

`crypto_address_identity` is an evidence-first address identity project. Its
first implementation phase supports Bitcoin mainnet only.

The project stores source observations, evidence, conflicts, and resolver
revisions locally. It can export an immutable, checksum-pinned resolver snapshot
for a consumer, but it does not run inside a collector and it does not control a
consumer's thresholds, cursor, quality gates, lake writes, or alert decisions.

## Current Boundary

- Local evidence ledger with bounded, explicitly approved live evidence imports.
- No live 0xRouter request is made without a separate explicit approval.
- Bitcoin is the only enabled provider chain.
- Ethereum, BSC, Solana, and Zcash are schema placeholders only.
- `quant_crypto` integration is read-only replay first and is a separate
  follow-up approval.
- Resolver policy is explicit: an uncontested Tier C provider entity may be a
  `provider_default`; append-only local corrections can select or reject an
  existing evidenced value; unresolved disagreements remain `conflict_first`.
- Identity replay never changes an existing alert, email, or suppression
  action. It reports coverage and counterfactual limits instead.

## Local Checks

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
python -m compileall -q src/crypto_address_identity
```

See `docs/specs/2026-07-22-btc-address-identity-development-spec.md` for the
data contract and `docs/plans/2026-07-22-btc-address-identity.md` for the
implementation sequence.

Operational procedure and the evidence-import contract are in
`docs/btc_identity_operations.md` and `docs/btc_identity_evidence_format.md`.
