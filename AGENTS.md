# AGENTS.md - crypto_address_identity Operating Rules

This repository (`/Users/barry/Documents/GitHub/crypto_address_identity`) is an
independent address-identity evidence service. Do not mix its runtime state,
source data, or consumer policy with `quant_crypto` or any other repository.

## Workspace Guard

Before reading or editing, confirm the current directory is exactly:

`/Users/barry/Documents/GitHub/crypto_address_identity`

If it differs, switch before continuing.

## Security And Data Boundary

1. Never print, commit, or place in generated docs any provider token, payment
   header, private key, cookie, signed URL, raw credential, or full private
   request/response payload.
2. Keep SQLite databases, raw provider payloads, resolver exports, logs, local
   state, and `configs/*.json` outside Git. Commit only schemas, code, sanitized
   fixtures, examples, and aggregate audit results.
3. Preserve source evidence and conflicts append-only. A local correction may
   select or reject existing evidence, but must not overwrite or erase it.
4. A resolver result does not itself control a consumer. Any alert, threshold,
   monitor, or suppression change requires a separately reviewed consumer
   change with its own audit trail.

## Shared Rules Skill

Canonical skill root: `skills/cai-shared-rules`.

Run the preflight before meaningful edits, a commit, a live provider request,
or a consumer-integration change:

```bash
python skills/cai-shared-rules/scripts/preflight.py \
  --workflow <workflow> --owned-path <path> [--owned-path <path>]
```

For high-impact or cross-project operations, refresh the global memory
injection first:

```bash
python /Users/barry/.codex/memory-hub/tools/codex_memory_discover.py \
  --cwd "$PWD" --task "<task summary>"
```

## Git Execution Protocol

1. Work on a focused `codex/` branch unless the user explicitly authorizes a
   direct `main` update.
2. Keep one coherent change per commit. Preserve unrelated dirty work and stage
   files explicitly; never use `git add -A` for a mixed worktree.
3. Before committing, review `git diff`, run `git diff --check`, run targeted
   tests plus the relevant compile check, and run preflight with
   `--check-staged` after staging.
4. Before pushing or merging, `git fetch origin`, reconcile with the current
   `origin/main`, rerun affected checks after a rebase/merge, and inspect the
   final staged diff.
5. Push only reviewed commits. Report the commit SHA, branch, validation
   commands, and intentionally excluded files.
6. Remote consumer or production changes must use a commit already in
   `origin/main`; never deploy a copied worktree or local runtime state.

## Review And Handoff

- Review for evidence provenance, resolver conflict behavior, immutability,
  snapshot checksum compatibility, and consumer non-interference.
- State whether provider calls, paid endpoints, or consumer behavior changed.
- When handing off, record the branch, exact commit, owned paths, tests run,
  unresolved conflicts, and any runtime state intentionally left outside Git.
