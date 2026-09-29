#!/usr/bin/env bash
# One-time registration of the npm package names, run by a maintainer who is
# logged in to npm (`npm login`) with publish rights on the @hyper-light scope.
#
# npm attaches a trusted publisher only to a package that already exists
# (npm/cli#8544: there are no pending publishers as on PyPI), so each of the
# nine names is created once, here, as a `0.0.0` placeholder that is deprecated
# on the spot; the release workflow then publishes every real version with
# OIDC and no token. After this script, register the trusted publisher on
# npmjs.com for each package: Settings → Trusted publisher → GitHub Actions,
# organization `hyper-light`, repository `focal`, workflow `release.yml`,
# environment left empty (scripts/release/README.md).
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
names=$(python3 - "$root" <<'EOF'
import json, sys
from pathlib import Path
root = Path(sys.argv[1]) / "packaging" / "npm"
print(json.loads((root / "package.json").read_text())["name"])
for platform in sorted(path.name for path in (root / "platforms").iterdir()):
    print(json.loads((root / "platforms" / platform / "package.json").read_text())["name"])
EOF
)

npm whoami >/dev/null
for name in $names; do
  if npm view "$name" name >/dev/null 2>&1; then
    echo "$name exists; nothing to register"
    continue
  fi
  directory=$(mktemp -d)
  cat > "$directory/package.json" <<EOF
{
  "name": "$name",
  "version": "0.0.0",
  "description": "Name registration for Focal's release workflow; install a released version",
  "repository": { "type": "git", "url": "git+https://github.com/hyper-light/focal.git" },
  "license": "MIT",
  "publishConfig": { "access": "public" }
}
EOF
  printf '# %s\n\nA placeholder that registers the name; the release workflow publishes every real version.\n' "$name" > "$directory/README.md"
  (cd "$directory" && npm publish --access public)
  npm deprecate "$name@0.0.0" "name registration only; install a released version"
  rm -rf "$directory"
  echo "registered $name"
done
