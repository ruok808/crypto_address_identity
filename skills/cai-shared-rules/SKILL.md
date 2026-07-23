---
name: cai-shared-rules
description: Shared Git, evidence-safety, and execution rules for crypto_address_identity. Use when starting or resuming work, preparing a commit, coordinating concurrent changes, running provider work, or planning a consumer integration.
---

# CAI Shared Rules

## Purpose

Keep `crypto_address_identity` changes reviewable, evidence-preserving, and
safe to hand to independent consumers.

## Hard Constraints

1. Work only from `/Users/barry/Documents/GitHub/crypto_address_identity`.
2. Never commit secrets, raw private provider payloads, runtime databases,
   generated resolver exports, logs, or local credentials.
3. Preserve evidence and conflicts append-only. Do not edit a historical
   snapshot, evidence record, or consumer output to force a conclusion.
4. A resolver snapshot is a read-only input. Consumer alert/suppression changes
   require a separate scoped implementation and audit trail.
5. Keep unrelated working-tree changes unstaged and intact.

## Ordered Workflow

1. For high-impact work, refresh memory:

```bash
python /Users/barry/.codex/memory-hub/tools/codex_memory_discover.py \
  --cwd "$PWD" --task "<task summary>"
```

2. Run preflight with narrow paths:

```bash
python skills/cai-shared-rules/scripts/preflight.py \
  --workflow <workflow> --owned-path src/crypto_address_identity
```

3. Before commit, run focused tests, a compile check for changed Python modules,
   and `git diff --check`.

4. Stage explicit paths, then validate the staged transaction:

```bash
git add <reviewed-path> [...]
python skills/cai-shared-rules/scripts/preflight.py \
  --workflow <workflow> --owned-path <path> --check-staged
```

5. Commit one coherent change. Fetch `origin`, reconcile with `origin/main`,
   rerun affected checks if the commit changed, then push.

## Preflight Contract

`scripts/preflight.py` verifies the repository root, workflow identifier,
declared paths, current worktree summary, and optionally the staged diff. It
does not modify Git state, call providers, or read runtime data.

Use `--strict-owned` only when an isolated worktree is expected to contain no
unrelated edits; otherwise it reports outside paths without blocking their
preservation.

## Completion Report

Report the exact commit, pushed branch, checks run, owned paths, whether any
provider/consumer behavior changed, and any intentionally uncommitted files.
