#!/usr/bin/env bash
# One-shot setup of the stack (service superset-init). Every step is idempotent, so the service can
# run again on every `docker compose up`:
#   1. Superset metadata database and admin user
#   2. Gold export (superset/data) -> PostgreSQL psi_gold, views psi_bi   (skipped when unchanged)
#   3. database connection and datasets, from superset/assets/datasets.yml
#   4. dashboards exported to superset/dashboards/*.zip, if any
set -euo pipefail

REPO="${PSI_REPO:-/repo}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"

echo "== 1/4 Superset metadata"
superset db upgrade
if ! superset fab list-users 2>/dev/null | grep -q "username:${ADMIN_USER} "; then
    superset fab create-admin --username "${ADMIN_USER}" --firstname Admin --lastname PSI \
        --email "${ADMIN_USER}@psi.local" --password "${SUPERSET_ADMIN_PASSWORD:?set SUPERSET_ADMIN_PASSWORD in .env}"
fi
superset init

echo "== 2/4 Gold export -> PostgreSQL"
python "${REPO}/superset/loader/load_gold.py"

echo "== 3/4 Database connection and datasets"
python "${REPO}/superset/assets/build_assets.py" --out /tmp/psi_datasets.zip
superset import-datasources -p /tmp/psi_datasets.zip -u "${ADMIN_USER}"

echo "== 4/4 Dashboards"
shopt -s nullglob
dashboards=("${REPO}"/superset/dashboards/*.zip)
for dashboard in "${dashboards[@]}"; do
    echo "Importing ${dashboard##*/}"
    superset import-dashboards -p "${dashboard}" -u "${ADMIN_USER}"
done
[ ${#dashboards[@]} -gt 0 ] || echo "No dashboard in superset/dashboards yet."

echo "Ready: http://localhost:${SUPERSET_PORT:-8088} (user ${ADMIN_USER})"
