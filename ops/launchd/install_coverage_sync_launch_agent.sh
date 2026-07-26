#!/bin/zsh

# Install the daily local coverage-sync LaunchAgent without starting a run.
set -euo pipefail

readonly label="com.ruok808.crypto-address-identity.coverage-sync"
readonly script_dir="${0:A:h}"
readonly project_root="${script_dir:h:h}"
readonly template_path="${script_dir}/${label}.plist.tmpl"
readonly worker_path="${script_dir}/coverage_sync_worker.sh"
readonly runtime_env_path="${CAI_RUNTIME_ENV_PATH:-${HOME}/.config/crypto_address_identity/coverage-sync.env}"
readonly python_bin="${CAI_PYTHON_BIN:-/Users/barry/.pyenv/versions/3.13.7/bin/python3}"
readonly launch_agents_dir="${HOME}/Library/LaunchAgents"
readonly runtime_worker_dir="${CAI_RUNTIME_WORKER_DIR:-${HOME}/Library/Application Support/crypto_address_identity/bin}"
readonly runtime_worker_path="${runtime_worker_dir}/coverage_sync_worker.sh"
readonly log_dir="${project_root}/logs"
readonly log_path="${log_dir}/coverage_sync_worker.log"
readonly target_path="${launch_agents_dir}/${label}.plist"
readonly uid="$(id -u)"

if [[ ! -x "$worker_path" || ! -r "$template_path" ]]; then
  print -u2 -- "coverage-sync launchd artifacts are incomplete"
  exit 78
fi

if [[ ! -x "$python_bin" ]]; then
  print -u2 -- "configured Python runtime is unavailable"
  exit 78
fi

if [[ ! -f "$runtime_env_path" ]]; then
  print -u2 -- "coverage-sync runtime env is missing"
  exit 78
fi

if [[ "$(stat -f '%Lp' "$runtime_env_path")" != "600" ]]; then
  print -u2 -- "coverage-sync runtime env must have mode 0600"
  exit 78
fi

if ! awk '
  NR == 1 && $0 ~ /^CAI_0XROUTER_TOKEN=.+$/ { valid = 1; next }
  { invalid = 1; exit }
  END { exit valid && !invalid ? 0 : 1 }
' "$runtime_env_path"; then
  print -u2 -- "coverage-sync runtime env must contain exactly one provider token assignment"
  exit 78
fi

if launchctl print "gui/${uid}/${label}" >/dev/null 2>&1; then
  print -u2 -- "coverage-sync LaunchAgent is already loaded; do not replace it during a run"
  exit 75
fi

mkdir -p "$launch_agents_dir" "$runtime_worker_dir" "$log_dir"
chmod 700 "$runtime_worker_dir" "$log_dir"

temporary_worker_path="${runtime_worker_path}.tmp.$$"
trap 'rm -f "$temporary_worker_path"' EXIT
cp "$worker_path" "$temporary_worker_path"
chmod 755 "$temporary_worker_path"
mv "$temporary_worker_path" "$runtime_worker_path"
trap - EXIT

"$python_bin" - "$template_path" "$target_path" "$runtime_worker_path" "$project_root" \
  "$runtime_env_path" "$python_bin" "$log_path" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path
from xml.sax.saxutils import escape

template_path, target_path, worker_path, project_root, runtime_env_path, python_bin, log_path = map(
    Path, sys.argv[1:]
)
replacements = {
    "__WORKER_PATH__": str(worker_path),
    "__PROJECT_ROOT__": str(project_root),
    "__RUNTIME_ENV_PATH__": str(runtime_env_path),
    "__PYTHON_BIN__": str(python_bin),
    "__LOG_PATH__": str(log_path),
}
payload = template_path.read_text(encoding="utf-8")
for marker, value in replacements.items():
    payload = payload.replace(marker, escape(value))
temporary_path = target_path.with_suffix(".plist.tmp")
temporary_path.write_text(payload, encoding="utf-8")
temporary_path.replace(target_path)
PY

chmod 644 "$target_path"
plutil -lint "$target_path" >/dev/null
launchctl bootstrap "gui/${uid}" "$target_path"
launchctl print "gui/${uid}/${label}" >/dev/null

print -- "coverage-sync LaunchAgent installed; next execution is scheduled daily at 03:20 local time"
