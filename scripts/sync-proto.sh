#!/usr/bin/env bash
# Compares proto/yukon.proto against the source copy in the yukon agent
# repo. With --apply, it copies the file over and regenerates the Go
# bindings. We chose manual sync over a shared proto package: there is
# one consumer (this repo), so a package adds versioning cost for no gain.
set -euo pipefail

agent_repo="${YUKON_AGENT_REPO:-../yukon}"
source_proto="$agent_repo/src/main/proto/yukon.proto"
dest_proto="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/proto/yukon.proto"

if [[ ! -f "$source_proto" ]]; then
  echo "error: $source_proto not found (set YUKON_AGENT_REPO to the agent repo's path)" >&2
  exit 1
fi

if diff -u "$dest_proto" "$source_proto"; then
  echo "proto/yukon.proto is already in sync"
  exit 0
fi

if [[ "${1:-}" != "--apply" ]]; then
  echo
  echo "schemas differ. Rerun with --apply to copy and regenerate." >&2
  exit 1
fi

cp "$source_proto" "$dest_proto"
protoc --go_out=. --go_opt=module=github.com/LukeDevOps/yukon-collector "proto/yukon.proto"
echo "synced and regenerated"
