# Superset with the PostgreSQL driver. The official image is "lean": database drivers are added
# on top, as the Superset docs recommend for custom images.
FROM apache/superset:6.1.0

USER root
RUN . /app/.venv/bin/activate \
    && (python -c "import psycopg2" 2>/dev/null \
        || uv pip install --no-cache psycopg2-binary==2.9.13 \
        || pip install --no-cache-dir psycopg2-binary==2.9.13) \
    && python -c "import psycopg2, yaml"
USER superset
