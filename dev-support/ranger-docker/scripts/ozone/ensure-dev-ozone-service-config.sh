#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Update dev_ozone service so OM Kerberos principal (om) can download policies.

set -euo pipefail

ADMIN_URL="${RANGER_ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${RANGER_ADMIN_USER:-admin}"
ADMIN_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
SERVICE="dev_ozone"
DOWNLOAD_USERS="ozone,om"

payload="$(curl -sf -u "${ADMIN_USER}:${ADMIN_PASS}" "${ADMIN_URL}/service/plugins/services/name/${SERVICE}" | python3 -c "
import json, sys
users = sys.argv[1]
svc = json.load(sys.stdin)
cfg = svc.setdefault('configs', {})
for k in ('policy.download.auth.users', 'tag.download.auth.users', 'userstore.download.auth.users'):
    cfg[k] = users
cfg['hadoop.security.authentication'] = 'kerberos'
print(json.dumps(svc))
" "${DOWNLOAD_USERS}")"

curl -sf -u "${ADMIN_USER}:${ADMIN_PASS}" -H 'Content-Type: application/json' \
  -X PUT "${ADMIN_URL}/service/plugins/services/${SERVICE}" \
  -d "${payload}" >/dev/null

echo "dev_ozone: policy.download.auth.users=${DOWNLOAD_USERS}"
