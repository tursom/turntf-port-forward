#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
turntf_dir="$(cd "${repo_dir}/../../turntf" && pwd)"
temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/turntf-port-forward-integration.XXXXXX")"
trap 'rm -rf "${temp_dir}"' EXIT

turntf_binary="${TURNTF_BIN:-${temp_dir}/turntf}"
if [[ -z "${TURNTF_BIN:-}" ]]; then
  (cd "${turntf_dir}" && go build -o "${turntf_binary}" ./cmd/turntf)
fi

port_forward_binary="${PORT_FORWARD_BIN:-${temp_dir}/turntf-port-forward}"
if [[ -z "${PORT_FORWARD_BIN:-}" ]]; then
  (cd "${repo_dir}" && GOWORK=off go build -o "${port_forward_binary}" ./cmd/turntf-port-forward)
fi

cd "${repo_dir}"
TURNTF_BIN="${turntf_binary}" PORT_FORWARD_BIN="${port_forward_binary}" GOWORK=off go test -tags integration ./internal/portforward -run '^TestRealTurntf' -count=1 -v
