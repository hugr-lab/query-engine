#!/bin/bash
set -euo pipefail

VERSION="${1:?Usage: $0 <version> (e.g. v0.4.0)}"

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: version must match vX.Y.Z format"
    exit 1
fi

ROOT="$(git rev-parse --show-toplevel)"
MOD="github.com/hugr-lab/query-engine"

# direct first — bypass proxy.golang.org cache for fresh tags;
# fallback to proxy for every other dependency.
export GOPROXY="direct,https://proxy.golang.org,direct"
# Disable parent workspace so `go get`/`go mod tidy` operate on this repo's
# go.mod instead of the outer go.work (which forces readonly mode).
export GOWORK=off

# Safety checks
[[ -z "$(git status --porcelain)" ]] || { echo "Error: working tree dirty"; exit 1; }
[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || { echo "Error: must be on main"; exit 1; }
git pull --ff-only

echo "Releasing $VERSION"

# ─── 1. types: leaf module, tag current HEAD ────────────────────────────
echo "▶ tagging types/$VERSION"
git tag "types/$VERSION"
git push origin "types/$VERSION"

# ─── 2. client: bump types@VERSION, build, commit, tag ──────────────────
echo "▶ bumping client → types@$VERSION"
cd "$ROOT/client"
go get "$MOD/types@$VERSION"
go mod tidy
go build ./...
cd "$ROOT"
git add client/go.mod client/go.sum
git commit -m "chore(client): bump types to $VERSION"
git push origin HEAD
git tag "client/$VERSION"
git push origin "client/$VERSION"

# ─── 3. root: bump types + client, build, commit, tag ───────────────────
echo "▶ bumping root → types@$VERSION client@$VERSION"
cd "$ROOT"
go get "$MOD/types@$VERSION" "$MOD/client@$VERSION"
go mod tidy
CGO_CFLAGS="-O1 -g" go build -tags=duckdb_arrow ./...
git add go.mod go.sum
git commit -m "chore: bump submodule deps to $VERSION"
git push origin HEAD
git tag "$VERSION"
git push origin "$VERSION"

echo ""
echo "✔ Released:"
echo "  types/$VERSION"
echo "  client/$VERSION"
echo "  $VERSION"
