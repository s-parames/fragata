#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for arg in "$@"; do
  if [[ "$arg" == "--department" || "$arg" == --department=* ]]; then
    echo "Do not pass --department to this wrapper. It always uses sistemas." >&2
    exit 1
  fi
done
exec bash "${SCRIPT_DIR}/onboard_db.sh" --department sistemas "$@"
