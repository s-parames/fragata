#!/usr/bin/env bash
set -euo pipefail

REMOTE_USER="${DAILY_INGEST_REMOTE_USER:-tec_app2}"
REMOTE_HOST="${DAILY_INGEST_REMOTE_HOST:-ft3.cesga.es}"
REMOTE_SSH_KEY="${DAILY_INGEST_REMOTE_SSH_KEY:-$HOME/.ssh/rag_hpc}"
REMOTE_WORKDIR="${DAILY_INGEST_REMOTE_WORKDIR:-/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag}"
REMOTE_COMPUTE_BIN="${DAILY_INGEST_REMOTE_COMPUTE_BIN:-compute}"
SSH_STRICT_HOST_KEY_CHECKING="${DAILY_INGEST_REMOTE_STRICT_HOST_KEY_CHECKING:-accept-new}"
SYNC_BACK_ENABLE_RAW="${DAILY_INGEST_SYNC_BACK_ENABLE:-1}"
SYNC_BACK_RELOAD_URL="${DAILY_INGEST_SYNC_BACK_RELOAD_URL:-}"
SYNC_BACK_RELOAD_TIMEOUT_SEC="${DAILY_INGEST_SYNC_BACK_RELOAD_TIMEOUT_SEC:-900}"
SYNC_BACK_RELOAD_REQUIRED_RAW="${DAILY_INGEST_SYNC_BACK_RELOAD_REQUIRED:-1}"

if [[ $# -lt 1 ]]; then
  cat >&2 <<'EOF'
Usage:
  bash scripts/compute_via_ft3.sh <compute args...>

Example:
  bash scripts/compute_via_ft3.sh -c 32 --mem 32G --gpu -- env MAIN_DAILY_INGEST_IN_COMPUTE=1 bash -lc '...'
EOF
  exit 1
fi

if [[ ! -f "$REMOTE_SSH_KEY" ]]; then
  echo "Remote SSH key not found: $REMOTE_SSH_KEY" >&2
  exit 1
fi

normalize_bool() {
  local raw="${1:-}"
  case "${raw,,}" in
    1|true|yes|y|on)
      echo "1"
      ;;
    *)
      echo "0"
      ;;
  esac
}

SYNC_BACK_ENABLE="$(normalize_bool "$SYNC_BACK_ENABLE_RAW")"
SYNC_BACK_RELOAD_REQUIRED="$(normalize_bool "$SYNC_BACK_RELOAD_REQUIRED_RAW")"

resource_args=()
payload_args=()
seen_separator=0
for arg in "$@"; do
  if [[ "$arg" == "--" && "$seen_separator" -eq 0 ]]; then
    seen_separator=1
    continue
  fi
  if [[ "$seen_separator" -eq 0 ]]; then
    resource_args+=("$arg")
  else
    payload_args+=("$arg")
  fi
done

if [[ ${#resource_args[@]} -eq 0 ]]; then
  echo "Missing compute resource arguments." >&2
  exit 1
fi

remote_resource_args="$(printf '%q ' "${resource_args[@]}")"
remote_resource_args="${remote_resource_args% }"
remote_workdir_quoted="$(printf '%q' "$REMOTE_WORKDIR")"
remote_compute_bin_quoted="$(printf '%q' "$REMOTE_COMPUTE_BIN")"

if [[ ${#payload_args[@]} -gt 0 ]]; then
  payload_line="$(printf '%q ' "${payload_args[@]}")"
  payload_line="${payload_line% }"
  payload_script="$(cat <<EOF
cd ${remote_workdir_quoted}
${payload_line}
__compute_payload_rc=\$?
exit \$__compute_payload_rc
EOF
)"
  payload_b64="$(printf '%s\n' "$payload_script" | base64 | tr -d '\n')"
  remote_cmd="cd ${remote_workdir_quoted} && printf '%s' '${payload_b64}' | base64 -d | ${remote_compute_bin_quoted} ${remote_resource_args}"
else
  remote_cmd="cd ${remote_workdir_quoted} && ${remote_compute_bin_quoted} ${remote_resource_args}"
fi

ssh_exec() {
  ssh \
    -i "$REMOTE_SSH_KEY" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking="$SSH_STRICT_HOST_KEY_CHECKING" \
    "${REMOTE_USER}@${REMOTE_HOST}" \
    bash -lc "$1"
}

sync_back_artifacts() {
  local ssh_transport
  ssh_transport="ssh -i ${REMOTE_SSH_KEY} -o BatchMode=yes -o StrictHostKeyChecking=${SSH_STRICT_HOST_KEY_CHECKING}"
  local remote_prefix
  remote_prefix="${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_WORKDIR}"

  echo "[sync-back] Syncing FAISS index from ${REMOTE_HOST}..."
  rsync -a --delete -e "$ssh_transport" \
    "${remote_prefix}/data/index/faiss_v2/" \
    "data/index/faiss_v2/"

  echo "[sync-back] Syncing dataset and watermark from ${REMOTE_HOST}..."
  rsync -a -e "$ssh_transport" \
    "${remote_prefix}/data/datasetFinalV2.jsonl" \
    "data/datasetFinalV2.jsonl"
  rsync -a -e "$ssh_transport" \
    "${remote_prefix}/state/last_success_ts.txt" \
    "state/last_success_ts.txt"
}

reload_local_engine_after_sync() {
  if [[ -z "$SYNC_BACK_RELOAD_URL" ]]; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "[sync-back] curl not found; cannot reload local engine endpoint=${SYNC_BACK_RELOAD_URL}" >&2
    if [[ "$SYNC_BACK_RELOAD_REQUIRED" == "1" ]]; then
      return 1
    fi
    return 0
  fi

  local tmp_body
  tmp_body="$(mktemp)"
  set +e
  local http_code
  http_code="$(curl -sS -m "$SYNC_BACK_RELOAD_TIMEOUT_SEC" -o "$tmp_body" -w "%{http_code}" -X POST "$SYNC_BACK_RELOAD_URL")"
  local curl_rc=$?
  set -e
  local body=""
  if [[ -f "$tmp_body" ]]; then
    body="$(cat "$tmp_body" 2>/dev/null || true)"
    rm -f "$tmp_body"
  fi

  if [[ "$curl_rc" -eq 0 && "$http_code" =~ ^2 ]]; then
    echo "[sync-back] Local engine reload OK (http=${http_code}) endpoint=${SYNC_BACK_RELOAD_URL}"
    if [[ -n "$body" ]]; then
      echo "[sync-back] Reload response: ${body}"
    fi
    return 0
  fi

  echo "[sync-back] Local engine reload failed (curl_rc=${curl_rc} http=${http_code}) endpoint=${SYNC_BACK_RELOAD_URL}" >&2
  if [[ -n "$body" ]]; then
    echo "[sync-back] Reload response: ${body}" >&2
  fi
  if [[ "$SYNC_BACK_RELOAD_REQUIRED" == "1" ]]; then
    return 1
  fi
  return 0
}

if [[ ${#payload_args[@]} -eq 0 ]]; then
  exec ssh_exec "$remote_cmd"
fi

set +e
ssh_exec "$remote_cmd"
remote_rc=$?
set -e
if [[ "$remote_rc" -ne 0 ]]; then
  exit "$remote_rc"
fi

if [[ "$SYNC_BACK_ENABLE" == "1" ]]; then
  sync_back_artifacts
  reload_local_engine_after_sync
fi

exit 0
