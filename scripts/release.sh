#!/bin/bash
set -euo pipefail

VERSION="${1:?Usage: $0 <version> (e.g. v0.4.0)}"

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: version must match vX.Y.Z format"
    exit 1
fi

echo "Creating release tags: $VERSION"

git tag "${VERSION}"
git tag "types/${VERSION}"
git tag "client/${VERSION}"

echo ""
echo "Tags created:"
echo "  ${VERSION}"
echo "  types/${VERSION}"
echo "  client/${VERSION}"
echo ""
echo "Push with:"
echo "  git push origin ${VERSION} types/${VERSION} client/${VERSION}"
