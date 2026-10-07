#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
#
# Install a released solstone journal into an isolated home under $DEMO_DIR
# (never the caller's own install or service), then build the verona demo
# journal with it. Usage: tools/verona/demo.sh [extra demo.py args]
#        tools/verona/demo.sh serve   (serve the latest build; see serve.py)
#   DEMO_DIR         default .demo
#   JOURNAL_VERSION  platform release to install (default: latest release)
#   GOOGLE_API_KEY   required; the demo is processed on a Google key
set -eu
here=$(cd "$(dirname "$0")/../.." && pwd)
demo_dir=$(mkdir -p "${DEMO_DIR:-$here/.demo}" && cd "${DEMO_DIR:-$here/.demo}" && pwd)
runtime="$demo_dir/runtime"
journal_env() {
    env -i HOME="$runtime/home" PATH="$runtime/home/.local/bin:/usr/local/bin:/usr/bin:/bin" LANG=C.UTF-8 "$@"
}
if [ "${1:-}" = serve ]; then
    journal_env "$here/.venv/bin/python" "$here/tools/verona/serve.py" --build "$demo_dir/latest"
    exit
fi
: "${GOOGLE_API_KEY:?GOOGLE_API_KEY is required}"
command -v minisign >/dev/null || { echo "minisign is required to verify the release" >&2; exit 1; }
mkdir -p "$runtime/home"
curl -fsSL https://solstone.app/install.sh -o "$runtime/install.sh"
version_args=""
[ -n "${JOURNAL_VERSION:-}" ] && version_args="--version $JOURNAL_VERSION"
# shellcheck disable=SC2086
env -i HOME="$runtime/home" PATH="$(dirname "$(command -v minisign)"):/usr/local/bin:/usr/bin:/bin" LANG=C.UTF-8 \
    sh "$runtime/install.sh" --components journal --prefix "$runtime/prefix" \
    --no-start --no-path --non-interactive --json $version_args > "$runtime/install.json"
out="$demo_dir/build-$(date +%Y%m%d-%H%M%S)"
journal_env GOOGLE_API_KEY="$GOOGLE_API_KEY" "$here/.venv/bin/python" "$here/tools/verona/demo.py" --out "$out" "$@"
ln -sfn "$out" "$demo_dir/latest"
echo "demo journal: $out/journal"
