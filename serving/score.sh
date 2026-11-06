#!/usr/bin/env bash
# Call the churn model after you create a serving endpoint called "churn-model"
# (Serving > Create serving endpoint > entity: workspace.sakura.churn_model, version: champion).
#   export DATABRICKS_HOST=https://<your-workspace-url>
#   export DATABRICKS_TOKEN=<a personal access token or OAuth token>
set -euo pipefail
curl -s -X POST "$DATABRICKS_HOST/serving-endpoints/churn-model/invocations" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @"$(dirname "$0")/request.json"
echo
