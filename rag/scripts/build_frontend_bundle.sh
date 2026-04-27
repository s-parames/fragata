#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build_frontend_bundle.sh [--output-dir DIR] [--api-base-url URL]

Builds a static frontend bundle from templates/index.html and static/.

Options:
  --output-dir DIR    Output directory (default: dist/frontend)
  --api-base-url URL  Absolute backend API URL (for split frontend/backend deploy)
  -h, --help          Show this help message
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${ROOT_DIR}/dist/frontend"
API_BASE_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      if [[ $# -lt 2 ]]; then
        echo "Error: --output-dir requires a value." >&2
        exit 1
      fi
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --api-base-url)
      if [[ $# -lt 2 ]]; then
        echo "Error: --api-base-url requires a value." >&2
        exit 1
      fi
      API_BASE_URL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'." >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "${API_BASE_URL}" && ! "${API_BASE_URL}" =~ ^https?:// ]]; then
  echo "Error: --api-base-url must start with http:// or https://." >&2
  exit 1
fi

API_BASE_URL="${API_BASE_URL%/}"

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}/static"
cp "${ROOT_DIR}/templates/index.html" "${OUTPUT_DIR}/index.html"
cp -r "${ROOT_DIR}/static/." "${OUTPUT_DIR}/static/"

# Optional root-level favicon for browsers that request /favicon.ico directly.
if [[ -f "${ROOT_DIR}/static/favicon.ico" ]]; then
  cp "${ROOT_DIR}/static/favicon.ico" "${OUTPUT_DIR}/favicon.ico"
fi

if [[ -n "${API_BASE_URL}" ]]; then
  escaped_base_url="$(printf '%s' "${API_BASE_URL}" | sed 's/[\/&]/\\&/g')"
  sed -i "s|<meta name=\"rag-api-base-url\" content=\"\" />|<meta name=\"rag-api-base-url\" content=\"${escaped_base_url}\" />|" "${OUTPUT_DIR}/index.html"
fi

echo "Frontend bundle generated at: ${OUTPUT_DIR}"
if [[ -n "${API_BASE_URL}" ]]; then
  echo "API base URL set to: ${API_BASE_URL}"
else
  echo "API base URL left empty (same-origin mode)."
fi
