#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Ensure audit ingestor allows hbase user for dev_hbase.
# Delegates to ensure-audit-ingestor-plugin-users.sh (full allowlist sync).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/hbase/ensure-hbase-ingestor-allowlist.sh
#   ./scripts/hbase/ensure-hbase-ingestor-allowlist.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" "$@"
