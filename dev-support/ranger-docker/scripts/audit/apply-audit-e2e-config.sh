#!/bin/bash
# Dev-only wrapper — use ./setup-audit-e2e.sh config instead.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "${SCRIPT_DIR}/setup-audit-e2e.sh" config "$@"
