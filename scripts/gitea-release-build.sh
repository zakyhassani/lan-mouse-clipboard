#!/usr/bin/env bash
# Build and package a release from an exact version tag.
#
# Usage:
#   scripts/gitea-release-build.sh                 # tag from HEAD
#   RELEASE_TAG=v0.11.0 scripts/gitea-release-build.sh
#
# The tag must be vX.Y.Z and match the version in Cargo.toml. The workflow in
# .gitea/workflows/gitea-release.yml only runs this script for tag pushes.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "error: $*" >&2
    exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo is not installed"
command -v python3 >/dev/null 2>&1 || fail "python3 is not installed"

tag="${RELEASE_TAG:-}"
if [ -z "$tag" ]; then
    tag="$(git describe --tags --exact-match 2>/dev/null || true)"
fi
[ -n "$tag" ] || fail "no exact tag at HEAD; pass RELEASE_TAG or check out a tag"

if ! [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)([-+][0-9A-Za-z.-]+)?$ ]]; then
    fail "tag '$tag' is not a version tag like v0.11.0"
fi

if [ "${ALLOW_DIRTY:-0}" != "1" ]; then
    if ! git diff --quiet || ! git diff --cached --quiet; then
        fail "working tree has uncommitted changes; commit them or set ALLOW_DIRTY=1"
    fi
fi

if [ "${ALLOW_DIRTY:-0}" != "1" ] && git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    tag_commit="$(git rev-list -n1 "refs/tags/$tag")"
    head_commit="$(git rev-parse HEAD)"
    [ "$tag_commit" = "$head_commit" ] || fail "tag '$tag' does not point at HEAD"
fi

version="${tag#v}"
cargo_version="$(
    cargo metadata --no-deps --format-version 1 | python3 -c '
import json, sys
packages = json.load(sys.stdin)["packages"]
print(next(p["version"] for p in packages if p["name"] == "lan-mouse"))
'
)"
[ "$version" = "$cargo_version" ] || fail "tag version '$version' does not match Cargo.toml version '$cargo_version'"

arch="$(uname -m)"
case "$arch" in
    x86_64 | amd64) arch="x86_64" ;;
    aarch64 | arm64) arch="aarch64" ;;
    *) fail "unsupported build architecture: $arch" ;;
esac

out_dir="${OUT_DIR:-target/gitea-release}"
name="lan-mouse-${tag}-linux-${arch}"
stage="$out_dir/$name"
archive="$name.tar.gz"

target_dir="${CARGO_TARGET_DIR:-target}"
echo "building lan-mouse $version from $tag on linux-$arch"
cargo build --release --locked -p lan-mouse

rm -rf "$out_dir"
mkdir -p "$stage"

[ -x "$target_dir/release/lan-mouse" ] || fail "missing $target_dir/release/lan-mouse"
install -m 0755 "$target_dir/release/lan-mouse" "$stage/lan-mouse"
for file in README.md LICENSE config.toml CLIPBOARD.md DOC.md; do
    [ -f "$file" ] || fail "missing release file: $file"
    install -m 0644 "$file" "$stage/$file"
done
[ -f service/lan-mouse.service ] || fail "missing service/lan-mouse.service"
install -m 0644 service/lan-mouse.service "$stage/lan-mouse.service"

{
    echo "name=lan-mouse"
    echo "version=$version"
    echo "tag=$tag"
    echo "commit=$(git rev-parse HEAD)"
    echo "rustc=$(rustc --version)"
    echo "platform=linux-$arch"
} > "$stage/BUILD-INFO"

epoch="${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}"
tar --sort=name \
    --mtime="@$epoch" \
    --owner=0 --group=0 --numeric-owner \
    -C "$out_dir" -cf - "$name" | gzip -9n > "$out_dir/$archive"

(
    cd "$out_dir"
    sha256sum "$archive" > SHA256SUMS
)

echo "built $out_dir/$archive"
cat "$out_dir/SHA256SUMS"
