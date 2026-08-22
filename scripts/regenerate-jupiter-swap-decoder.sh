#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/jupiter-swap-latest-decoder}"
RPC_URL="${RPC_URL:-https://api.mainnet-beta.solana.com}"
PROGRAM_ID="${PROGRAM_ID:-JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4}"

cd "${REPO_ROOT}"
rm -rf "${OUT_DIR}"

node packages/cli/dist/cli.js parse \
  --idl "${PROGRAM_ID}" \
  --url "${RPC_URL}" \
  --out-dir "${OUT_DIR}" \
  --name jupiter-swap \
  --standard anchor \
  --with-postgres true \
  --with-graphql true \
  --with-serde true \
  --standalone false \
  --package-version 1.0.0

find "${OUT_DIR}/src" -name '*.rs' -print0 | xargs -0 rustfmt --edition 2021

echo "Generated latest Jupiter swap decoder at ${OUT_DIR}"
echo "Review semantic diff with:"
echo "  diff -ru decoders/jupiter-swap-decoder/src ${OUT_DIR}/src"
