"""Checks of the Power BI project (powerbi/, PBIP format: TMDL semantic model + PBIR report).

Power BI Desktop does not run in CI, so these tests check what can break silently when the text files
are edited by hand: every field used by a visual exists in the model, every measure lives in the
measure table, DAX references, relationships and sort-by columns point to existing objects, and the
Power Query reads only the Gold export files listed in the manifest of notebook 21.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PBI = ROOT / "powerbi"
MODEL = PBI / "PSI_Lakehouse.SemanticModel" / "definition"
REPORT = PBI / "PSI_Lakehouse.Report" / "definition"
MEASURE_TABLE = "_Measures"


def _unquote(name: str) -> str:
    name = name.strip()
    return name[1:-1].replace("''", "'") if name.startswith("'") and name.endswith("'") else name


def _parse_tables() -> dict:
    """{table: {"columns": {name: props}, "measures": {name: dax}, "partition": m}} from tables/*.tmdl."""
    tables = {}
    for path in sorted((MODEL / "tables").glob("*.tmdl")):
        lines = path.read_text(encoding="utf-8").split("\n")
        table = None
        current = None
        for line in lines:
            if line.startswith("table "):
                table = _unquote(line[len("table "):])
                tables[table] = {"columns": {}, "measures": {}, "partition": ""}
            elif line.startswith("\tcolumn "):
                current = ("columns", _unquote(line[len("\tcolumn "):]))
                tables[table]["columns"][current[1]] = {}
            elif line.startswith("\tmeasure "):
                name, _, dax = line[len("\tmeasure "):].partition(" =")
                current = ("measures", _unquote(name))
                tables[table]["measures"][current[1]] = dax.strip()
            elif line.startswith("\tpartition "):
                current = ("partition", None)
            elif line.startswith("\t\t\t") and current:
                if current[0] == "measures":
                    tables[table]["measures"][current[1]] += "\n" + line.strip()
                elif current[0] == "partition":
                    tables[table]["partition"] += line.strip() + "\n"
            elif line.startswith("\t\t") and current and current[0] == "columns" and ":" in line:
                key, _, value = line.strip().partition(":")
                tables[table]["columns"][current[1]][key] = value.strip()
    return tables


TABLES = _parse_tables()


def _visuals() -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(REPORT.glob("pages/*/visuals/*/visual.json"))]


def _fields(node) -> list[tuple[str, str, str]]:
    """Every (kind, entity, property) of Column/Measure references with an Entity in a JSON tree."""
    found = []
    if isinstance(node, dict):
        for kind in ("Column", "Measure"):
            ref = node.get(kind)
            if isinstance(ref, dict) and "Entity" in ref.get("Expression", {}).get("SourceRef", {}):
                found.append((kind, ref["Expression"]["SourceRef"]["Entity"], ref["Property"]))
        for value in node.values():
            found += _fields(value)
    elif isinstance(node, list):
        for value in node:
            found += _fields(value)
    return found


def test_project_files_present():
    assert (PBI / "PSI_Lakehouse.pbip").is_file()
    for name in ("model.tmdl", "database.tmdl", "relationships.tmdl", "expressions.tmdl"):
        assert (MODEL / name).is_file(), name
    assert (REPORT / "report.json").is_file() and (REPORT / "pages" / "pages.json").is_file()
    assert set(TABLES) == {"State", "Area", "Date", "CVLI", "Profile item", MEASURE_TABLE}


def test_every_json_file_declares_its_schema():
    for path in PBI.rglob("*"):
        if path.suffix in (".json", ".pbir", ".pbism", ".pbip") and "StaticResources" not in path.parts:
            doc = json.loads(path.read_text(encoding="utf-8"))
            assert doc.get("$schema", "").startswith("https://developer.microsoft.com/json-schemas/fabric/"), path


def test_all_measures_live_in_the_measure_table():
    for table, spec in TABLES.items():
        if table != MEASURE_TABLE:
            assert not spec["measures"], f"measures outside {MEASURE_TABLE}: {table}"
    assert len(TABLES[MEASURE_TABLE]["measures"]) >= 10
    text = (MODEL / "tables" / f"{MEASURE_TABLE}.tmdl").read_text(encoding="utf-8")
    assert "column Dummy\n\t\tdataType: string\n\t\tisHidden" in text   # only hidden columns: measure table icon


def test_measure_names_do_not_clash_with_columns():
    measures = set(TABLES[MEASURE_TABLE]["measures"])
    columns = {c for spec in TABLES.values() for c in spec["columns"]}
    assert not measures & columns, measures & columns


def test_dax_references_exist():
    measures = set(TABLES[MEASURE_TABLE]["measures"])
    for name, dax in TABLES[MEASURE_TABLE]["measures"].items():
        code = re.sub(r'"[^"]*"', '""', dax)   # drop string literals ("Month index" in SELECTCOLUMNS)
        for table, column in re.findall(r"('[^']+'|\b[A-Za-z_][\w ]*?)\[([^\]]+)\]", code):
            table = _unquote(table.strip())
            assert table in TABLES, f"{name}: unknown table {table}"
            assert column in TABLES[table]["columns"], f"{name}: unknown column {table}[{column}]"
        bare = re.findall(r"(?<![\w'\]])\[([^\]]+)\]", code)
        for ref in bare:
            if ref.startswith("@"):   # column added in the measure itself (ADDCOLUMNS)
                assert f'"{ref}"' in dax, f"{name}: [{ref}] is not defined in the measure"
                continue
            assert ref in measures, f"{name}: unknown measure [{ref}]"


def test_visual_fields_exist_in_the_model():
    visuals = _visuals()
    assert len(visuals) >= 12
    for visual in visuals:
        for kind, entity, prop in _fields(visual):
            assert entity in TABLES, f"{visual['name']}: unknown table {entity}"
            if kind == "Measure":
                assert entity == MEASURE_TABLE and prop in TABLES[entity]["measures"], f"{visual['name']}: [{prop}]"
            else:
                assert prop in TABLES[entity]["columns"], f"{visual['name']}: {entity}[{prop}]"


def test_relationships_and_sort_columns_exist():
    text = (MODEL / "relationships.tmdl").read_text(encoding="utf-8")
    ends = re.findall(r"(?:fromColumn|toColumn): (.+)", text)
    assert len(ends) == 6
    for end in ends:
        table, _, column = end.partition(".")
        assert _unquote(column) in TABLES[_unquote(table)]["columns"], end
    for table, spec in TABLES.items():
        for column, props in spec["columns"].items():
            if "sortByColumn" in props:
                assert _unquote(props["sortByColumn"]) in spec["columns"], f"{table}[{column}]"


def test_power_query_reads_only_exported_gold_tables():
    import importlib.util

    spec = importlib.util.spec_from_file_location("nb21", ROOT / "notebooks" / "21_gold_export_bi.py")
    source = (ROOT / "notebooks" / "21_gold_export_bi.py").read_text(encoding="utf-8")
    exported = set(re.findall(r'^\s+"(\w+)": \[', source, flags=re.M))
    assert spec is not None and exported
    m_code = (MODEL / "expressions.tmdl").read_text(encoding="utf-8") + "".join(
        t["partition"] for t in TABLES.values())
    used = set(re.findall(r'GoldTable\("(\w+)"\)', m_code))
    assert used and used <= exported, used - exported


@pytest.mark.parametrize("page", ["overview"])
def test_page_lists_its_visuals(page):
    pages = json.loads((REPORT / "pages" / "pages.json").read_text(encoding="utf-8"))
    assert page in pages["pageOrder"]
    names = {v["name"] for v in _visuals()}
    folders = {p.name for p in (REPORT / "pages" / page / "visuals").iterdir()}
    assert names == folders
