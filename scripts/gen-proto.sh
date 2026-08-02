#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_dir}"

# 从 proto/tunnel.proto 重新生成 Go 协议代码。
command -v protoc >/dev/null 2>&1 || { echo "错误：未找到 protoc" >&2; exit 1; }
command -v protoc-gen-go >/dev/null 2>&1 || { echo "错误：未找到 protoc-gen-go" >&2; exit 1; }

protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=module=github.com/tursom/turntf-port-forward \
  tunnel.proto
