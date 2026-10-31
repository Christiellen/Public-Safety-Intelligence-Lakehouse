"""Runs Databricks source-format notebooks (notebooks/*.py) locally, the way %run does on Databricks.

A notebook is split into cells on the "# COMMAND ----------" separator. Markdown cells and magic
commands (lines starting with "# MAGIC", e.g. %pip) are skipped. A "%run ./00_config_utils" cell
re-runs the configuration notebook followed by the local overrides, so every notebook sees the same
namespace, as it does on Databricks. dbutils.notebook.exit() ends the notebook, as on Databricks.
"""

from pathlib import Path
from types import SimpleNamespace

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"
SEPARATOR = "\n# COMMAND ----------\n"

# Local storage: no Unity Catalog (schemas in the Spark warehouse) and Parquet instead of Delta
LOCAL_OVERRIDES = {"CATALOG": None, "TABLE_FORMAT": "parquet"}


class NotebookExit(Exception):
    """Raised by the fake dbutils.notebook.exit(); ends the notebook being run."""


def _exit(value=None):
    raise NotebookExit(value)


def _no_widget(name):
    raise ValueError(f"No widget named {name}")


fake_dbutils = SimpleNamespace(
    notebook=SimpleNamespace(exit=_exit),
    widgets=SimpleNamespace(get=_no_widget),
    library=SimpleNamespace(restartPython=lambda: None),
    secrets=SimpleNamespace(get=lambda scope, key: "secret"),
)


def cells(name: str) -> list[str]:
    """Source of every cell of a notebook, markdown and magics included."""
    text = (NOTEBOOKS / name).read_text(encoding="utf-8")
    text = text.removeprefix("# Databricks notebook source\n")
    return [cell.strip("\n") for cell in text.split(SEPARATOR)]


def code_cells(name: str) -> list[str]:
    """Code cells and %run cells (markdown and other magics, such as %pip, are left out)."""
    return [c for c in cells(name) if not c.lstrip().startswith("# MAGIC") or "%run" in c.split("\n")[0]]


def run_config(namespace: dict) -> None:
    for cell in code_cells("00_config_utils.py"):
        if not cell.lstrip().startswith("# MAGIC"):
            exec(compile(cell, "00_config_utils", "exec"), namespace)
    namespace.update(LOCAL_OVERRIDES)
    namespace.update(namespace.get("__overrides__", {}))  # test doubles (e.g. http_get) and settings


def run_notebook(name: str, namespace: dict, replace: dict | None = None) -> str | None:
    """Runs every code cell. Returns the message of dbutils.notebook.exit(), or None when the notebook
    ran to the end. `replace` maps source fragments to substitutes (e.g. a path that does not exist
    locally)."""
    namespace.setdefault("dbutils", fake_dbutils)
    try:
        for cell in code_cells(name):
            if cell.lstrip().startswith("# MAGIC"):
                if "%run ./00_config_utils" in cell:
                    run_config(namespace)
                    continue
                if "%run ./17_agents_multiagent" in cell:
                    run_notebook("17_agents_multiagent.py", namespace)
                    continue
                raise ValueError(f"Unsupported magic in {name}: {cell.splitlines()[0]}")
            for old, new in (replace or {}).items():
                cell = cell.replace(old, new)
            exec(compile(cell, name, "exec"), namespace)
    except NotebookExit as stop:
        return str(stop)
    return None


def run_cell(name: str, namespace: dict, contains: str) -> None:
    """Runs the single code cell that contains `contains`."""
    matches = [c for c in code_cells(name) if contains in c]
    if len(matches) != 1:
        raise ValueError(f"{len(matches)} cells of {name} contain {contains!r}")
    exec(compile(matches[0], name, "exec"), namespace)
