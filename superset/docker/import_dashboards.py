"""Imports the dashboards exported to superset/dashboards/*.zip, replacing charts that already exist.

    superset-init runs: python import_dashboards.py <zip> [<zip> ...]

`superset import-dashboards` overwrites the dashboard itself but never a chart that already exists
(same UUID): a change to a chart in the ZIP (format, tooltip, axis) would not reach a stack created
before it. So each ZIP is imported twice, as in the Superset UI with "overwrite": first its charts
(ImportChartsCommand, overwrite=True), then the dashboard. Databases and datasets in the ZIP are
only created when missing: they come from step 3 of init.sh (assets/datasets.yml).
"""

from __future__ import annotations

import os
import sys
from zipfile import ZipFile

import yaml


def chart_bundle(contents: dict[str, str]) -> dict[str, str]:
    """The dashboard bundle reduced to a chart bundle: charts, datasets and databases, with the
    metadata type the chart importer expects."""
    keep = ("charts/", "datasets/", "databases/")
    bundle = {path: text for path, text in contents.items() if path.startswith(keep)}
    metadata = yaml.safe_load(contents["metadata.yaml"])
    metadata["type"] = "Slice"
    bundle["metadata.yaml"] = yaml.safe_dump(metadata)
    return bundle


def main(paths: list[str]) -> int:
    from flask import g
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset.commands.chart.importers.v1 import ImportChartsCommand
        from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
        from superset.commands.importers.v1.utils import get_contents_from_bundle

        from superset import security_manager

        username = os.environ.get("SUPERSET_ADMIN_USER", "admin")
        g.user = security_manager.find_user(username=username)
        if g.user is None:
            print(f"User {username} not found", file=sys.stderr)
            return 1
        for path in paths:
            with ZipFile(path) as zip_file:
                contents = get_contents_from_bundle(zip_file)
            charts = chart_bundle(contents)
            ImportChartsCommand(charts, overwrite=True).run()
            ImportDashboardsCommand(contents, overwrite=True).run()
            count = sum(1 for p in charts if p.startswith("charts/"))
            print(f"Imported {os.path.basename(path)}: dashboard and {count} charts (replaced)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
