"""The Databricks job (resources/psi_pipeline.job.yml) against the real data dependencies.

The dependencies are read from the notebooks themselves, so a new table read or a new input of
run_or_exit() that is not reflected in the job order makes this test fail:
- every table a notebook reads must be written by an upstream task;
- every dataset in a notebook's run_or_exit() must be logged by an upstream task.
"""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = ROOT / "notebooks"
JOB = ROOT / "resources" / "psi_pipeline.job.yml"
SCHEDULED = [f"{n:02d}" for n in range(1, 17)]  # 01-16; the agents (17-19) run on demand


@pytest.fixture(scope="module")
def tasks():
    job = yaml.safe_load(JOB.read_text())["resources"]["jobs"]["psi_pipeline"]
    result = {}
    for task in job["tasks"]:
        path = (JOB.parent / task["notebook_task"]["notebook_path"]).resolve()
        deps = [d["task_key"] for d in task.get("depends_on", [])]
        result[task["task_key"]] = {"notebook": path, "deps": deps}
    return result


def ancestors(tasks, key, seen=None):
    seen = set() if seen is None else seen
    for dep in tasks[key]["deps"]:
        if dep not in seen:
            seen.add(dep)
            ancestors(tasks, dep, seen)
    return seen


def source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def written_tables(text: str) -> set:
    constants = dict(re.findall(r'^(\w+) = "(\w+)"', text, flags=re.M))
    variables = {var: (layer, name) for var, layer, name in re.findall(r'(\w+) = tbl\("(\w+)", "(\w+)"\)', text)}
    found = set(re.findall(r'write_table\([^,]+, "(\w+)", "(\w+)"', text))
    for layer, name in re.findall(r'replace_partitions\([^,]+, "(\w+)", (\w+|"\w+")', text):
        found.add((layer, constants.get(name, name.strip('"'))))
    found |= {("bronze", n) for n in re.findall(r'write_if_changed\("(\w+)"', text)}
    found |= {variables[v] for v in re.findall(r"saveAsTable\((\w+)\)", text) if v in variables}
    return found


def read_tables(text: str) -> set:
    return set(re.findall(r'tbl\("(\w+)", "(\w+)"\)', text)) - written_tables(text)


def logged_datasets(text: str) -> set:
    constants = dict(re.findall(r'^(\w+) = "(\w+)"', text, flags=re.M))
    names = {constants.get(n, n.strip('"')) for n in re.findall(r'log_change\((\w+|"\w+")', text)}
    return names | set(re.findall(r'write_if_changed\("(\w+)"', text))


def consumed_datasets(text: str) -> set:
    match = re.search(r"run_or_exit\(CONSUMER, \[([^\]]*)\]", text)
    return set(re.findall(r'"(\w+)"', match.group(1))) if match else set()


def test_schedules_notebooks_01_to_16_once(tasks):
    prefixes = sorted(t["notebook"].name[:2] for t in tasks.values())
    assert prefixes == SCHEDULED
    for task in tasks.values():
        assert task["notebook"].exists(), task["notebook"]
        assert source(task["notebook"]).startswith("# Databricks notebook source")


def test_dependencies_form_a_dag(tasks):
    for key, task in tasks.items():
        assert set(task["deps"]) <= set(tasks), f"{key} depends on an unknown task"
        assert key not in ancestors(tasks, key), f"cycle through {key}"


def test_every_table_read_is_written_upstream(tasks):
    writers = {}
    for key, task in tasks.items():
        for table in written_tables(source(task["notebook"])):
            writers[table] = key
    dictionary = re.findall(r'^    "(\w+)": \{', source(NOTEBOOKS / "16_gold_data_dictionary.py"), flags=re.M)
    problems = []
    for key, task in tasks.items():
        reads = read_tables(source(task["notebook"]))
        if task["notebook"].name.startswith("16"):
            reads |= {("gold", name) for name in dictionary}
        for table in reads:
            writer = writers.get(table)
            if writer and writer != key and writer not in ancestors(tasks, key):
                problems.append(f"{key} reads {table} written by {writer}, which is not upstream")
    assert not problems, "\n".join(problems)


def test_every_change_log_input_is_produced_upstream(tasks):
    producers = {}
    for key, task in tasks.items():
        for dataset in logged_datasets(source(task["notebook"])):
            producers[dataset] = key
    problems = []
    for key, task in tasks.items():
        for dataset in consumed_datasets(source(task["notebook"])):
            producer = producers.get(dataset)
            if producer is None:
                problems.append(f"{key} waits for {dataset}, which no task logs")
            elif producer not in ancestors(tasks, key):
                problems.append(f"{key} waits for {dataset} logged by {producer}, which is not upstream")
    assert not problems, "\n".join(problems)


def test_the_lineage_checks_see_the_main_dependencies(tasks):
    """Guards the regular expressions above: if they stop matching, the checks would pass vacuously."""
    by_prefix = {t["notebook"].name[:2]: t for t in tasks.values()}
    assert ("silver", "dim_municipality") in read_tables(source(by_prefix["07"]["notebook"]))
    assert ("bronze", "bronze_sinesp_occurrences") in written_tables(source(by_prefix["02"]["notebook"]))
    assert ("bronze", "bronze_ibge_mesh_files") in written_tables(source(by_prefix["11"]["notebook"]))
    assert "silver_sinesp" in logged_datasets(source(by_prefix["07"]["notebook"]))
    assert consumed_datasets(source(by_prefix["14"]["notebook"])) == {
        "gold_spatial", "gold_municipality_analytics", "gold_dimensions"}


def test_job_parameter_matches_the_config_widget(tasks):
    job = yaml.safe_load(JOB.read_text())["resources"]["jobs"]["psi_pipeline"]
    assert [p["name"] for p in job["parameters"]] == ["full_refresh"]
    assert 'dbutils.widgets.get("full_refresh")' in source(NOTEBOOKS / "00_config_utils.py")
