"""Data dictionary of the BI semantic layer (views of psi_bi, as Superset datasets).

    PSI_READER_DSN=postgresql://psi_reader:<password>@localhost:5432/psi python superset/assets/bi_dictionary.py

Writes docs/data_dictionary/bi: Markdown, Excel and two CSV files (columns and metrics). The content is
the import bundle of build_assets.py, so it shows exactly what Superset receives: the real column
types of the views, the descriptions (from datasets.yml or the Gold dictionary) and the metric
formulas. tests/test_superset_stack.py fails when the committed files are out of date; regenerate
with this script or with UPDATE_DOCS=1 in that test.
"""

from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

import build_assets

DOCS = Path(__file__).resolve().parents[2] / "docs" / "data_dictionary" / "bi"
COLUMNS_CSV, METRICS_CSV = DOCS / "bi_columns.csv", DOCS / "bi_metrics.csv"
XLSX_FILE, MD_FILE = DOCS / "bi_dictionary.xlsx", DOCS / "README.md"
COLUMN_HEADER = ["dataset", "column_name", "label", "data_type", "description"]
METRIC_HEADER = ["dataset", "metric_name", "label", "format", "expression", "description"]


def build(reader_dsn: str) -> dict:
    """{"datasets": [(name, description, main_dttm_col)], "columns": [...], "metrics": [...]} in the
    order of datasets.yml."""
    spec = build_assets.load_spec()
    dictionary = build_assets.load_dictionary(build_assets.DEFAULT_DICTIONARY)
    view_columns = build_assets.read_view_columns(reader_dsn, list(spec["datasets"]))
    return from_bundle(spec, build_assets.build_bundle(spec, view_columns, dictionary, reader_dsn))


def from_bundle(spec: dict, files: dict) -> dict:
    configs = {c["table_name"]: c for path, c in files.items() if "/datasets/" in path}
    result = {"datasets": [], "columns": [], "metrics": []}
    for name in spec["datasets"]:
        config = configs[name]
        result["datasets"].append([name, config["description"] or "", config["main_dttm_col"] or ""])
        result["columns"] += [[name, c["column_name"], c["verbose_name"] or "", c["type"].lower(), c["description"]]
                              for c in config["columns"]]
        result["metrics"] += [[name, m["metric_name"], m["verbose_name"] or "", m["d3format"] or "",
                               m["expression"], m["description"] or ""] for m in config["metrics"]]
    return result


def _csv(header, rows) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue()


def render_columns_csv(doc) -> str:
    return _csv(COLUMN_HEADER, doc["columns"])


def render_metrics_csv(doc) -> str:
    return _csv(METRIC_HEADER, doc["metrics"])


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def render_markdown(doc) -> str:
    lines = [
        "# Data dictionary - BI semantic layer",
        "",
        "Views of the schema `psi_bi` in PostgreSQL, registered as Superset datasets by the Superset stack",
        "([`superset/`](../../../superset/README.md)). Superset reads only these views, with a read-only role.",
        "Generated from `superset/assets/datasets.yml` and the real view columns (`superset/assets/bi_dictionary.py`);",
        "do not edit by hand. The Gold tables behind the views are in the [Gold data dictionary](../README.md).",
        "",
        "Download: [Excel](bi_dictionary.xlsx) - [CSV columns](bi_columns.csv) - [CSV metrics](bi_metrics.csv)",
        "",
        "| Dataset | Columns | Metrics | Time column | Description |",
        "|---|---|---|---|---|",
    ]
    for name, description, dttm in doc["datasets"]:
        columns = sum(1 for c in doc["columns"] if c[0] == name)
        metrics = sum(1 for m in doc["metrics"] if m[0] == name)
        lines.append(f"| [{name}](#{name}) | {columns} | {metrics} | {dttm or '-'} | {_cell(description)} |")
    for name, description, _ in doc["datasets"]:
        lines += ["", f"## {name}", "", _cell(description), "",
                  "| Column | Label | Type | Description |", "|---|---|---|---|"]
        lines += [f"| `{c[1]}` | {_cell(c[2])} | {c[3]} | {_cell(c[4])} |" for c in doc["columns"] if c[0] == name]
        metrics = [m for m in doc["metrics"] if m[0] == name]
        if metrics:
            lines += ["", "| Metric | Label | Format | Expression | Description |", "|---|---|---|---|---|"]
            lines += [f"| `{m[1]}` | {_cell(m[2])} | `{m[3]}` | `{_cell(m[4])}` | {_cell(m[5])} |" for m in metrics]
    return "\n".join(lines) + "\n"


def write_xlsx(doc, path: Path) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.worksheet.table import Table, TableStyleInfo

    workbook = Workbook()
    sheets = [
        ("Datasets", ["dataset", "description", "time_column"], doc["datasets"], [30, 110, 14]),
        ("Columns", COLUMN_HEADER, doc["columns"], [28, 30, 32, 18, 100]),
        ("Metrics", METRIC_HEADER, doc["metrics"], [28, 30, 32, 10, 70, 80]),
    ]
    for index, (title, header, data, widths) in enumerate(sheets):
        sheet = workbook.active if index == 0 else workbook.create_sheet()
        sheet.title = title
        sheet.append(header)
        for row in data:
            sheet.append(row)
        for column, width in zip("ABCDEF", widths, strict=False):
            sheet.column_dimensions[column].width = width
        for cells in sheet.iter_rows(min_row=1):
            for cell in cells:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
        for cell in sheet[1]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="087F8C")
        sheet.freeze_panes = "A2"
        last = f"{chr(ord('A') + len(header) - 1)}{len(data) + 1}"
        table = Table(displayName=f"bi_{title.lower()}", ref=f"A1:{last}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        sheet.add_table(table)
    workbook.properties.creator = "Public-Safety-Intelligence-Lakehouse"
    workbook.save(path)


def read_xlsx(path: Path) -> dict:
    from openpyxl import load_workbook

    workbook = load_workbook(path, read_only=True)

    def rows(sheet):
        return [[c if c is not None else "" for c in r] for r in workbook[sheet].iter_rows(min_row=2, values_only=True)]

    return {"datasets": rows("Datasets"), "columns": rows("Columns"), "metrics": rows("Metrics")}


def write_all(doc) -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    COLUMNS_CSV.write_text(render_columns_csv(doc), encoding="utf-8")
    METRICS_CSV.write_text(render_metrics_csv(doc), encoding="utf-8")
    MD_FILE.write_text(render_markdown(doc), encoding="utf-8")
    write_xlsx(doc, XLSX_FILE)


def main() -> int:
    dsn = os.environ.get("PSI_READER_DSN")
    if not dsn:
        print("PSI_READER_DSN is not set", file=sys.stderr)
        return 2
    doc = build(dsn)
    write_all(doc)
    print(f"Wrote {DOCS}: {len(doc['datasets'])} datasets, {len(doc['columns'])} columns, {len(doc['metrics'])} metrics")
    return 0


if __name__ == "__main__":
    sys.exit(main())
