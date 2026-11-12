"""Superset settings for the local stack (docker-compose.yml). Secrets come from the environment
(.env), never from this file."""

import os
from urllib.parse import quote

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Superset's own metadata (users, charts, dashboards) in the database "superset" of the same server
_user = quote(os.environ.get("POSTGRES_USER", "postgres"), safe="")
_password = quote(os.environ["POSTGRES_PASSWORD"], safe="")
_host = os.environ.get("POSTGRES_HOST", "db")
_port = os.environ.get("POSTGRES_PORT", "5432")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{_user}:{_password}@{_host}:{_port}/superset"

# Single-node demo: in-process caches instead of Redis
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 600}
FILTER_STATE_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 86400}
EXPLORE_FORM_DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 86400}
RATELIMIT_STORAGE_URI = "memory://"

# The analytics database is read-only for Superset: no DML, uploads or CTAS (also enforced by the
# role psi_reader in PostgreSQL)
PREVENT_UNSAFE_DB_CONNECTIONS = True
SQLLAB_TIMEOUT = 60
ROW_LIMIT = 50000

# Optional: deck.gl maps of the municipality centroids need a Mapbox token
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# Colors of the dashboard prototype: teal for the measures, amber for the period reference line
EXTRA_CATEGORICAL_COLOR_SCHEMES = [{
    "id": "psiColors",
    "label": "Public Safety Intelligence",
    "description": "Teal for measures, amber for references",
    "isDefault": False,
    "colors": ["#087F8C", "#D98B1A", "#173B5C", "#5FA8B0", "#63748A", "#B4433F"],
}]
