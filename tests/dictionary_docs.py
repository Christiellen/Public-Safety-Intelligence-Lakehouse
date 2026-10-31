"""Published data dictionary (docs/data_dictionary): CSV, Excel and Markdown versions of psi_gold.data_dictionary.

The files are generated from the dictionary table that notebook 16 builds, so they show the same
descriptions and the real data types. test_data_dictionary.py fails when the committed files are out of
date; to regenerate them run:

    UPDATE_DOCS=1 pytest tests/test_data_dictionary.py -k published
"""

import csv
import io
from pathlib import Path

DOCS = Path(__file__).resolve().parents[1] / "docs" / "data_dictionary"
CSV_FILE, XLSX_FILE, MD_FILE = DOCS / "data_dictionary.csv", DOCS / "data_dictionary.xlsx", DOCS / "README.md"
HEADER = ["table_name", "column_name", "data_type", "description"]


def rows_of(dictionary_df, order: dict) -> list[list[str]]:
    """Rows of psi_gold.data_dictionary (a pandas DataFrame) in the order of the notebook's DICTIONARY:
    each table row followed by its columns."""
    pairs = [(t, "" if c == "_table" else c) for t, cols in order.items() for c in cols]
    rank = {pair: i for i, pair in enumerate(pairs)}
    rows = [[r.table_name, r.column_name or "", r.data_type or "", r.description] for r in dictionary_df.itertuples()]
    return sorted(rows, key=lambda r: rank[(r[0], r[1])])


def render_csv(rows) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(HEADER)
    writer.writerows(rows)
    return buffer.getvalue()


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def render_markdown(rows) -> str:
    tables = [(r[0], r[3]) for r in rows if not r[1]]
    lines = [
        "# Data dictionary - Gold layer",
        "",
        "Tables and columns of the Gold layer (`workspace.psi_gold`) that the analytics and the AI agents use.",
        "Generated from `psi_gold.data_dictionary` (notebook 16); do not edit by hand.",
        "",
        "Download: [Excel](data_dictionary.xlsx) - [CSV](data_dictionary.csv)",
        "",
        "| Table | Columns | Description |",
        "|---|---|---|",
    ]
    for name, description in tables:
        count = sum(1 for r in rows if r[0] == name and r[1])
        lines.append(f"| [{name}](#{name}) | {count} | {_cell(description)} |")
    for name, description in tables:
        lines += ["", f"## {name}", "", _cell(description), "", "| Column | Type | Description |", "|---|---|---|"]
        lines += [f"| `{r[1]}` | {r[2]} | {_cell(r[3])} |" for r in rows if r[0] == name and r[1]]
    return "\n".join(lines) + "\n"


def write_xlsx(rows, path: Path) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.worksheet.table import Table, TableStyleInfo

    workbook = Workbook()
    sheets = [
        ("Tables", ["table_name", "columns", "description"],
         [[r[0], sum(1 for c in rows if c[0] == r[0] and c[1]), r[3]] for r in rows if not r[1]], [30, 10, 110]),
        ("Columns", HEADER, [r for r in rows if r[1]], [30, 30, 12, 110]),
    ]
    for index, (title, header, data, widths) in enumerate(sheets):
        sheet = workbook.active if index == 0 else workbook.create_sheet()
        sheet.title = title
        sheet.append(header)
        for row in data:
            sheet.append(row)
        for column, width in zip("ABCD", widths, strict=False):
            sheet.column_dimensions[column].width = width
        for cells in sheet.iter_rows(min_row=1):
            for cell in cells:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
        for cell in sheet[1]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="1F4E79")
        sheet.freeze_panes = "A2"
        last = f"{chr(ord('A') + len(header) - 1)}{len(data) + 1}"
        table = Table(displayName=f"dictionary_{title.lower()}", ref=f"A1:{last}")
        table.tableStyleInfo = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        sheet.add_table(table)
    workbook.properties.creator = "Public-Safety-Intelligence-Lakehouse"
    workbook.save(path)


def read_xlsx_columns(path: Path) -> list[list[str]]:
    from openpyxl import load_workbook

    sheet = load_workbook(path, read_only=True)["Columns"]
    return [[c if c is not None else "" for c in row] for row in sheet.iter_rows(min_row=2, values_only=True)]


def write_all(rows) -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    CSV_FILE.write_text(render_csv(rows), encoding="utf-8")
    MD_FILE.write_text(render_markdown(rows), encoding="utf-8")
    write_xlsx(rows, XLSX_FILE)
