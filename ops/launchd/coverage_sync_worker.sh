#!/bin/zsh

# Run one bounded Chaindata coverage-sync batch under launchd.
set -euo pipefail

: "${CAI_PROJECT_ROOT:?CAI_PROJECT_ROOT is required}"
: "${CAI_RUNTIME_ENV_PATH:?CAI_RUNTIME_ENV_PATH is required}"
: "${CAI_PYTHON_BIN:?CAI_PYTHON_BIN is required}"

if [[ ! -f "$CAI_RUNTIME_ENV_PATH" ]]; then
  print -u2 -- "coverage-sync runtime env is missing"
  exit 78
fi

if [[ "$(stat -f '%Lp' "$CAI_RUNTIME_ENV_PATH")" != "600" ]]; then
  print -u2 -- "coverage-sync runtime env must have mode 0600"
  exit 78
fi

if ! awk '
  NR == 1 && $0 ~ /^CAI_0XROUTER_TOKEN=.+$/ { valid = 1; next }
  { invalid = 1; exit }
  END { exit valid && !invalid ? 0 : 1 }
' "$CAI_RUNTIME_ENV_PATH"; then
  print -u2 -- "coverage-sync runtime env must contain exactly one provider token assignment"
  exit 78
fi

token_line="$(<"$CAI_RUNTIME_ENV_PATH")"
export CAI_0XROUTER_TOKEN="${token_line#CAI_0XROUTER_TOKEN=}"

export PYTHONPATH="$CAI_PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
cd "$CAI_PROJECT_ROOT"
exec "$CAI_PYTHON_BIN" -m crypto_address_identity coverage-sync run
