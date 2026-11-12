# Dashboards

Superset dashboard exports (ZIP), imported on every start of the stack. Export a dashboard from the *Dashboards* list of Superset (the export action of the dashboard) and save the ZIP here with a stable name.

The exports refer to the datasets by UUID; `superset/assets/build_assets.py` gives the database and the datasets the same UUIDs in every environment, so the files import anywhere.
