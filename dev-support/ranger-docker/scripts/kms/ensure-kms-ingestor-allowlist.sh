#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
#
# Ensure audit ingestor allows rangerkms user for dev_kms.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/kms/ensure-kms-ingestor-allowlist.sh
#   ./scripts/kms/ensure-kms-ingestor-allowlist.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" "$@"
