# Databricks notebook source
# MAGIC %md
# MAGIC # 17 - Agents - Multi-agent assistant (LangGraph + Gemini)
# MAGIC
# MAGIC Answers questions in natural language over the Gold tables (Databricks SQL) and the municipality graph (Neo4j). This notebook only **defines** the assistant; notebooks 18 (demo) and 19 (evaluation) load it with `%run`.
# MAGIC
# MAGIC **Architecture**
# MAGIC ```
# MAGIC question ─▶ supervisor ─┬─▶ sql_analyst   (Gold tables, Spark SQL)  ─┐
# MAGIC                         └─▶ graph_analyst (Neo4j, Cypher)           ─┴─▶ reporter ─▶ answer
# MAGIC ```
# MAGIC - **supervisor** decides which specialists the question needs (one, both in parallel, or none when it is out of scope);
# MAGIC - each **analyst** is a tool-calling loop: the model writes a query, the tool runs it, and errors go back to the model so it can correct itself (at most `MAX_TOOL_CALLS` queries);
# MAGIC - the **reporter** writes the final answer **only from the query results**, saying which source each figure came from.
# MAGIC
# MAGIC **Guardrails (enforced in code, not only in the prompt)**
# MAGIC
# MAGIC | Risk | Control |
# MAGIC |---|---|
# MAGIC | SQL that writes or reads outside the project | `sqlglot` parses the query: a single `SELECT`, only tables in `psi_gold.data_dictionary` (notebook 16), no table functions such as `read_files`; `LIMIT` forced to `MAX_ROWS` |
# MAGIC | Cypher that writes | read transaction (`execute_read`) **and** a keyword blocklist (`CREATE`, `MERGE`, `SET`, `DELETE`, `CALL`, `LOAD CSV`...) |
# MAGIC | Reading other projects in the shared Neo4j | every node pattern that introduces a variable or a label must carry the `:PSI` label |
# MAGIC | Long or runaway queries | timeouts on Neo4j, row caps on both tools, tool-call limit per analyst |
# MAGIC | Secrets | Gemini and Neo4j credentials come only from the secret scope `psi` |
# MAGIC | Traceability | every run (question, route, queries, answer, tokens, latency, errors) is appended to `psi_gold.agent_runs` |
# MAGIC
# MAGIC **Model and quota.** The assistant uses a chain of Gemini Flash models (`GEMINI_MODEL_PREFERENCE`): the first available one answers, and the next ones take over when it is overloaded (503) or out of quota (429). A client-side rate limiter keeps the calls under the free-tier limit per minute (`GEMINI_REQUESTS_PER_MINUTE`). The model that actually answered is recorded in each run.
# MAGIC
# MAGIC **Requirements:** the Gemini API key in the secret `psi/gemini-api-key` (Google AI Studio), Neo4j loaded by notebook 14 and the dictionary from notebook 16. The calling notebook installs the libraries (see notebook 18).

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import json
import operator
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Annotated, Literal, TypedDict

import logging

import sqlglot
from sqlglot import exp

logging.getLogger("sqlglot").setLevel(logging.ERROR)   # rejected statements are reported by the guard
# Gemini 3.x models use fixed sampling and ignore temperature; TEMPERATURE still applies to models that accept it
import warnings
warnings.filterwarnings("ignore", message=".*uses fixed sampling defaults.*")
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field, ValidationError

SECRET_SCOPE = NEO4J_SECRET_SCOPE
GEMINI_SECRET_KEY = "gemini-api-key"
# Models in order of preference; the first available one is the primary and the next ones are
# fallbacks, used when the primary is overloaded (503) or out of quota (429). A model that the API
# reports as not found (404, e.g. retired for new keys) is skipped for the rest of the session.
# Check the limits of your key at https://ai.dev/rate-limit and adjust.
GEMINI_MODEL_PREFERENCE = ["gemini-3.6-flash", "gemini-3.5-flash", "gemini-flash-latest"]
GEMINI_FALLBACK_MODELS = 2       # how many fallbacks after the primary
# Client-side throttle: stay under the per-minute quota instead of hitting 429 and waiting.
# Free tier: 5 to 10 requests per minute depending on the model. Raise it on a paid tier.
GEMINI_REQUESTS_PER_MINUTE = 60
GEMINI_TIMEOUT_SECONDS = 120
TEMPERATURE = 0
MAX_ROWS = 50                  # rows returned by any query to the model
MAX_TOOL_CALLS = 6             # queries per analyst and question
MAX_RESULT_CHARS = 6000        # result text sent back to the model
NEO4J_TIMEOUT_SECONDS = 30
RUNS_TABLE = "agent_runs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Model

# COMMAND ----------

_STABLE_FLASH = re.compile(r"^gemini-(\d+(?:\.\d+)?)-flash$")


def model_chain(available: list) -> list:
    """Primary model followed by fallbacks: the preference list first, then the stable Flash models
    from newest to oldest. Lite, image, TTS, audio, live and preview variants are skipped."""
    chain = [m for m in GEMINI_MODEL_PREFERENCE if m in available]
    stable = sorted(((float(m.group(1)), m.group(0)) for m in map(_STABLE_FLASH.match, available) if m),
                    reverse=True)
    chain += [name for _, name in stable if name not in chain]
    if not chain:
        raise RuntimeError(f"No Gemini Flash model available. Models: {available}")
    return chain[:1 + GEMINI_FALLBACK_MODELS]


def available_models(api_key: str) -> list:
    from google import genai
    # Keep a reference to the client while the pager is read: google-genai closes the HTTP
    # connection when an unreferenced client is garbage collected ("client has been closed").
    client = genai.Client(api_key=api_key)
    try:
        models = list(client.models.list())
    finally:
        client.close()
    return [m.name.removeprefix("models/") for m in models
            if m.supported_actions is None or "generateContent" in m.supported_actions]


def build_llms():
    """One chat model per model of the chain, all sharing the same rate limiter."""
    from langchain_core.rate_limiters import InMemoryRateLimiter
    from langchain_google_genai import ChatGoogleGenerativeAI
    api_key = dbutils.secrets.get(SECRET_SCOPE, GEMINI_SECRET_KEY)
    chain = model_chain(available_models(api_key))
    limiter = InMemoryRateLimiter(requests_per_second=GEMINI_REQUESTS_PER_MINUTE / 60,
                                  check_every_n_seconds=0.5, max_bucket_size=1)
    llms = [ChatGoogleGenerativeAI(model=name, google_api_key=api_key, temperature=TEMPERATURE,
                                   max_retries=1, timeout=GEMINI_TIMEOUT_SECONDS, rate_limiter=limiter)
            for name in chain]
    return llms, chain

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data dictionary and SQL guard

# COMMAND ----------

dictionary_rows = spark.table(tbl("gold", "data_dictionary")).collect()
ALLOWED_TABLES = sorted({r.table_name for r in dictionary_rows})


def schema_prompt() -> str:
    lines = []
    for table in ALLOWED_TABLES:
        rows = [r for r in dictionary_rows if r.table_name == table]
        lines.append(f"\n### {tbl('gold', table)}\n{next(r.description for r in rows if r.column_name is None)}")
        lines += [f"- {r.column_name} ({r.data_type}): {r.description}" for r in rows if r.column_name]
    return "\n".join(lines)


class GuardError(ValueError):
    """Query rejected by a guardrail. The message goes back to the model."""


_GOLD_PARTS = tbl("gold", "x").split(".")[:-1]   # [catalog, schema] or [schema]


def guard_sql(query: str) -> str:
    """Returns a safe version of the query or raises GuardError."""
    try:
        statements = [s for s in sqlglot.parse(query, read="databricks") if s is not None]
    except sqlglot.errors.ParseError as error:
        raise GuardError(f"SQL could not be parsed: {error}") from None
    if len(statements) != 1:
        raise GuardError("Send exactly one SQL statement.")
    tree = statements[0]
    if not isinstance(tree, exp.Query):
        raise GuardError("Only SELECT queries are allowed.")
    forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter,
                 exp.Command, exp.Into, exp.Set)
    if any(isinstance(node, forbidden) for node in tree.walk()):
        raise GuardError("Only SELECT queries are allowed.")

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise GuardError("Table functions are not allowed; query the Gold tables directly.")
        name = table.name.lower()
        if not table.db and name in cte_names:
            continue
        schema_ok = not table.db or table.db.lower() == SCHEMAS["gold"]
        catalog_ok = not table.catalog or (CATALOG is not None and table.catalog.lower() == CATALOG.lower())
        if name not in ALLOWED_TABLES or not schema_ok or not catalog_ok:
            raise GuardError(f"Table {table.sql()} is not allowed. Allowed tables: "
                             + ", ".join(tbl("gold", t) for t in ALLOWED_TABLES))
        qualified = exp.to_table(tbl("gold", name))
        for part in ("this", "db", "catalog"):
            table.set(part, qualified.args.get(part))

    limit = tree.args.get("limit")
    current = None
    if limit is not None and isinstance(limit.expression, exp.Literal) and limit.expression.is_int:
        current = int(limit.expression.this)
    if current is None or current > MAX_ROWS:
        tree = tree.limit(MAX_ROWS)
    return tree.sql(dialect="databricks")


def execute_sql(query: str) -> pd.DataFrame:
    return spark.sql(query).limit(MAX_ROWS).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cypher guard

# COMMAND ----------

_CYPHER_WRITE = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|LOAD|CALL|FOREACH|GRANT|DENY|REVOKE|ALTER|RENAME|USE|TERMINATE)\b"
    r"|\b(apoc|dbms|gds|db)\s*\.", re.IGNORECASE)
_STRINGS = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_COMMENTS = re.compile(r"//[^\n]*|/\*.*?\*/", re.DOTALL)
# Node pattern: "(" [variable] [":Label..."] followed by "{" or ")".
_NODE = re.compile(r"\(\s*([A-Za-z_]\w*)?\s*((?::\s*`?[A-Za-z_]\w*`?\s*(?:[:&|]\s*`?[A-Za-z_]\w*`?\s*)*)?)\s*(?=[{)])")
# A "(" right after a word is a function call (count(n), nodes(p)), unless the word is a clause keyword
_PATTERN_KEYWORDS = {"MATCH", "WHERE", "AND", "OR", "XOR", "NOT", "RETURN", "WITH", "EXISTS", "OPTIONAL"}


def _node_patterns(text: str):
    for match in _NODE.finditer(text):
        before = re.search(r"(\w+)\s*$", text[:match.start()])
        if before and before.group(1).upper() not in _PATTERN_KEYWORDS:
            continue
        yield match.group(1), match.group(2)


def guard_cypher(query: str) -> str:
    text = _COMMENTS.sub(" ", query)
    without_strings = _STRINGS.sub("''", text)
    if ";" in without_strings.strip().rstrip(";"):
        raise GuardError("Send exactly one Cypher statement.")
    found = _CYPHER_WRITE.search(without_strings)
    if found:
        raise GuardError(f"Read-only access: '{found.group(0)}' is not allowed.")
    bound = set()
    for variable, labels in _node_patterns(without_strings):
        label_set = set(re.findall(r"[A-Za-z_]\w*", labels))
        if label_set and NEO4J_PROJECT_LABEL not in label_set:
            raise GuardError(f"Every labeled node must include :{NEO4J_PROJECT_LABEL}, "
                             f"e.g. (m:{NEO4J_PROJECT_LABEL}:Municipality).")
        if variable and variable not in bound:
            if not label_set:
                raise GuardError(f"Variable '{variable}' is introduced without the :{NEO4J_PROJECT_LABEL} label.")
            bound.add(variable)
    if not bound:
        raise GuardError(f"The query must match at least one :{NEO4J_PROJECT_LABEL} node.")
    return text.strip().rstrip(";")


_neo4j_driver = None


def execute_cypher(query: str) -> pd.DataFrame:
    """Runs in a READ transaction: the database itself rejects any write that passes the guard."""
    global _neo4j_driver
    from neo4j import GraphDatabase, unit_of_work
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(
            dbutils.secrets.get(SECRET_SCOPE, "neo4j-uri"),
            auth=(dbutils.secrets.get(SECRET_SCOPE, "neo4j-user"),
                  dbutils.secrets.get(SECRET_SCOPE, "neo4j-password")))

    # Inside a transaction function the timeout is set with @unit_of_work (Query objects with a
    # timeout are accepted only by session.run).
    @unit_of_work(timeout=NEO4J_TIMEOUT_SECONDS)
    def read(tx):
        result = tx.run(query)
        return [record.data() for record in result.fetch(MAX_ROWS)]

    with _neo4j_driver.session(default_access_mode="READ") as session:
        return pd.DataFrame(session.execute_read(read))


GRAPH_MODEL = f"""
Nodes (every node also has the label :{NEO4J_PROJECT_LABEL}):
- (:{NEO4J_PROJECT_LABEL}:Municipality) properties: code (IBGE 7-digit, string), name, population, cvli_victims,
  cvli_rate_100k, cvli_z_score_state, cvli_z_score_national, is_state_hotspot, is_national_hotspot, ivsc_score,
  idhm, gdp_per_capita_brl, economic_tier, fleet_total, centroid_lat, centroid_lon, area_km2, neighbor_count,
  neighbor_avg_cvli_rate_100k, neighbor_national_hotspots, lisa_cluster, lisa_p_value, eb_rate_std, degree,
  betweenness. The state code is NOT a municipality property: use the IN_STATE relationship.
- (:{NEO4J_PROJECT_LABEL}:State) properties: code (UF, e.g. 'PE'), name, region
- (:{NEO4J_PROJECT_LABEL}:Community) properties: id, municipalities, states, population, cvli_victims, cvli_rate_100k
Relationships:
- (Municipality)-[:BORDERS {{border_km}}]-(Municipality)  shared border; stored once, always query WITHOUT direction
- (Municipality)-[:IN_STATE]->(State)
- (Municipality)-[:IN_COMMUNITY]->(Community)  Louvain community of similar neighboring municipalities
lisa_cluster values: High-High, Low-Low, High-Low, Low-High, Not significant, No neighbors, No data.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tools

# COMMAND ----------

def to_text(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "The query returned no rows."
    text = frame.to_csv(index=False)
    if len(text) > MAX_RESULT_CHARS:
        text = text[:MAX_RESULT_CHARS] + "\n... (truncated)"
    return f"{len(frame)} row(s) (max {MAX_ROWS}):\n{text}"


@tool
def run_sql(query: str) -> str:
    """Runs one read-only Spark SQL SELECT on the Gold tables and returns the rows as CSV."""
    return to_text(execute_sql(guard_sql(query)))


@tool
def run_cypher(query: str) -> str:
    """Runs one read-only Cypher query on the municipality graph in Neo4j and returns the rows as CSV."""
    return to_text(execute_cypher(guard_cypher(query)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agents and graph

# COMMAND ----------

COMMON_RULES = """
Rules:
- Use only the data returned by your queries. Never invent numbers, names or columns.
- Municipality names repeat across states: always return the state code with a municipality name.
- Names are stored with accents (e.g. 'São Paulo'). To search a name, compare lower(name) and also accept
  the version without accents if the first query returns nothing.
- If a query fails, read the error, fix the query and try again.
- When you have the data, reply with a short factual summary in English with the figures, the
  reference period when available, and the query you used. Do not write the final answer for the user.
"""

SQL_PROMPT = f"""You are the SQL analyst of a public safety lakehouse for Brazil (Databricks, Spark SQL).
Answer the question by calling the tool run_sql. Use fully qualified table names.
The only tables you can query are described below.
{{schema}}

Metric notes:
- CVLI = lethal intentional violent crimes; cvli_rate_100k is the main violence indicator.
- For rankings by rate, ignore small municipalities unless asked (population >= 20000), because small
  populations produce unstable rates. Say when you applied this filter.
- Vehicle crimes and drug seizures exist only by state (state_analytics, fact_state_occurrences).
- You do NOT have the list of borders between municipalities: the graph analyst answers that part.
  Never approximate neighbors with coordinates, distances or communities, and never claim that two
  municipalities share a border. If the question needs neighbors, return only the indicators asked
  for the municipalities named in the question and say that the borders come from the graph.
{COMMON_RULES}"""

GRAPH_PROMPT = f"""You are the graph analyst of a public safety project for Brazil (Neo4j, Cypher).
Answer the question by calling the tool run_cypher. The graph connects neighboring municipalities.
{GRAPH_MODEL}
Use the graph for questions about neighbors, borders, contagion/spillover between municipalities,
paths, clusters crossing state borders and communities. Every node pattern must include :{NEO4J_PROJECT_LABEL}.
{COMMON_RULES}"""

SUPERVISOR_PROMPT = """You route questions about public safety and socioeconomic indicators of Brazilian
municipalities and states to specialists.
- "sql": rankings, totals, rates, comparisons, indicators of municipalities or states, time series by month,
  vehicle crimes, drug seizures, IDHM, GDP, fleet, population, LISA cluster counts.
- "graph": neighbors and borders between municipalities, spillover/contagion, paths, clusters that cross
  state borders, communities of neighboring municipalities.
Choose both when the question needs both. Choose none when the question is outside this scope
(e.g. weather, sports, personal advice) or asks to change, delete or create data.
Also name the language the question is written in (e.g. "English", "Portuguese", "Spanish").
Reply ONLY with JSON: {"agents": ["sql" | "graph", ...], "reason": "<one sentence>", "language": "<language>"}"""

REPORTER_PROMPT = """You write the final answer for the user of a public safety analytics assistant for Brazil.
- Write the WHOLE answer, including notes and caveats, in {language}. Be concise.
- Use ONLY the findings below. Do not add figures, names or facts that are not in them.
- If the graph analyst could not answer, do not present a list of neighbors from other findings as if
  they were confirmed borders; say that the neighborhood information is unavailable.
- If the findings do not answer the question, say so plainly and say what data is available.
- Mention the reference period when the findings give it, and which source supports each figure
  (Gold tables or neighborhood graph). Round rates to one decimal.
- The data are police records sent by each state to SINESP, so they reflect registered crimes, and
  underreporting and recording rules differ between states. When the answer compares or ranks states
  (or municipalities of different states), add one short sentence with this caveat. CVLI is the most
  reliable indicator; say so if useful.
- When no specialist was called because the question is out of scope or asks to change data, explain
  politely that the assistant only answers read-only questions about public safety in Brazil."""


class Route(BaseModel):
    agents: list[Literal["sql", "graph"]] = Field(default_factory=list)
    reason: str = ""
    language: str = ""


class AgentState(TypedDict, total=False):
    question: str
    route: list
    route_reason: str
    language: str
    findings: Annotated[list, operator.add]
    trace: Annotated[list, operator.add]
    answer: str


def text_of(message: AIMessage) -> str:
    """Plain text of a model reply. Gemini can return a list of parts; `.text` joins the text parts.
    It returns a str subclass in recent langchain-core, so it is converted to a real str (Spark needs one)."""
    return str(message.text)


def usage_of(message: AIMessage) -> dict:
    """Tokens and the model that actually answered (the primary or a fallback)."""
    usage = getattr(message, "usage_metadata", None) or {}
    model = (getattr(message, "response_metadata", None) or {}).get("model_name")
    return {"input_tokens": usage.get("input_tokens", 0), "output_tokens": usage.get("output_tokens", 0),
            "model": model}


def parse_route(text: str) -> Route:
    """Reads the supervisor JSON. If it is not valid, both specialists are called (safe fallback)."""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    try:
        return Route.model_validate(json.loads(match.group(0)))
    except (AttributeError, json.JSONDecodeError, ValidationError):
        return Route(agents=["sql", "graph"], reason="Fallback: supervisor output was not valid JSON.")


class ModelChain:
    """Calls the models in order and returns the first answer.
    - Overload (503), quota (429) or timeout: tries the next model for this call.
    - Not found (404): the model is retired for the rest of the session (shared by all agents).
    - If every model fails, the error lists the reason of each one."""

    def __init__(self, names: list, runnables: list, retired: set):
        self.names, self.runnables, self.retired = names, runnables, retired

    def invoke(self, messages):
        errors = []
        for name, runnable in zip(self.names, self.runnables, strict=True):
            if name in self.retired:
                continue
            try:
                return runnable.invoke(messages)
            except Exception as error:
                text = str(error)
                if "NOT_FOUND" in text or "404" in text[:200]:
                    self.retired.add(name)
                errors.append(f"{name}: {type(error).__name__}: {text[:300]}")
        raise RuntimeError("Every Gemini model failed:\n" + "\n".join(errors or ["all models retired (404)"]))


def build_assistant(llms: list):
    """llms: primary model first, then fallbacks."""
    names = [str(getattr(m, "model", f"model_{i}")).removeprefix("models/") for i, m in enumerate(llms)]
    retired = set()
    llm = ModelChain(names, llms, retired)
    sql_llm = ModelChain(names, [m.bind_tools([run_sql]) for m in llms], retired)
    graph_llm = ModelChain(names, [m.bind_tools([run_cypher]) for m in llms], retired)
    sql_prompt = SQL_PROMPT.replace("{schema}", schema_prompt())

    def supervisor(state: AgentState):
        response = llm.invoke([SystemMessage(SUPERVISOR_PROMPT), HumanMessage(state["question"])])
        route = parse_route(text_of(response))
        agents = list(dict.fromkeys(route.agents))
        return {"route": agents, "route_reason": route.reason, "language": route.language,
                "trace": [{"agent": "supervisor", "type": "route", "content": json.dumps(agents),
                           **usage_of(response)}]}

    def analyst(name, bound_llm, system_prompt, tool_fn):
        def node(state: AgentState):
            messages = [SystemMessage(system_prompt), HumanMessage(state["question"])]
            trace, calls = [], 0
            while True:
                response = bound_llm.invoke(messages)
                messages.append(response)
                trace.append({"agent": name, "type": "llm", "content": text_of(response)[:2000],
                              **usage_of(response)})
                if not response.tool_calls:
                    return {"findings": [{"agent": name, "summary": text_of(response)}], "trace": trace}
                for call in response.tool_calls:
                    calls += 1
                    query = call["args"].get("query", "")
                    if calls > MAX_TOOL_CALLS:
                        result, status = "Tool-call limit reached. Summarize what you have.", "limit"
                    else:
                        try:
                            result, status = tool_fn.invoke({"query": query}), "ok"
                        except GuardError as error:
                            result, status = f"Rejected by guardrail: {error}", "blocked"
                        except Exception as error:   # query errors go back to the model to self-correct
                            result, status = f"Query error: {str(error)[:800]}", "error"
                    trace.append({"agent": name, "type": "tool", "content": query, "status": status,
                                  "result": result[:1000]})
                    messages.append(ToolMessage(content=result, tool_call_id=call["id"]))
                if calls > MAX_TOOL_CALLS + 1:
                    return {"findings": [{"agent": name, "summary": "Stopped: tool-call limit reached."}],
                            "trace": trace}
        return node

    def reporter(state: AgentState):
        findings = state.get("findings", [])
        material = "\n\n".join(f"[{f['agent']}]\n{f['summary']}" for f in findings) or \
            f"No specialist was called. Supervisor reason: {state.get('route_reason', '')}"
        # The answer language comes from the supervisor, so it does not drift with the language of
        # the findings (written in English by the analysts) or of the data (Portuguese names).
        language = state.get("language") or "the same language as the question"
        response = llm.invoke([SystemMessage(REPORTER_PROMPT.replace("{language}", language)),
                               HumanMessage(f"Question: {state['question']}\n\nFindings:\n{material}")])
        return {"answer": text_of(response),
                "trace": [{"agent": "reporter", "type": "llm", "content": text_of(response)[:2000],
                           **usage_of(response)}]}

    def dispatch(state: AgentState):
        targets = [{"sql": "sql_analyst", "graph": "graph_analyst"}[a] for a in state.get("route", [])]
        return targets or ["reporter"]

    graph = StateGraph(AgentState)
    graph.add_node("supervisor", supervisor)
    graph.add_node("sql_analyst", analyst("sql_analyst", sql_llm, sql_prompt, run_sql))
    graph.add_node("graph_analyst", analyst("graph_analyst", graph_llm, GRAPH_PROMPT, run_cypher))
    graph.add_node("reporter", reporter)
    graph.add_edge(START, "supervisor")
    graph.add_conditional_edges("supervisor", dispatch, ["sql_analyst", "graph_analyst", "reporter"])
    graph.add_edge("sql_analyst", "reporter")
    graph.add_edge("graph_analyst", "reporter")
    graph.add_edge("reporter", END)
    return graph.compile()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Entry point and audit log
# MAGIC
# MAGIC `ask()` runs one question and appends the run to `psi_gold.agent_runs`. The table keeps the question, the route, every query with its status (`ok`, `error`, `blocked`, `limit`), the answer, tokens and latency, so the assistant can be audited and its cost tracked.

# COMMAND ----------

RUN_SCHEMA = """run_id string, run_at timestamp, model string, question string, route string, answer string,
sql_queries array<string>, cypher_queries array<string>, blocked_queries int, failed_queries int,
input_tokens bigint, output_tokens bigint, latency_seconds double, status string, error string,
trace string"""

_assistant = None
_model_name = None


def get_assistant():
    global _assistant, _model_name
    if _assistant is None:
        llms, chain = build_llms()
        _model_name = chain[0]
        _assistant = build_assistant(llms)
        print(f"Assistant ready - model {chain[0]} - fallbacks {chain[1:] or 'none'} - "
              f"max {GEMINI_REQUESTS_PER_MINUTE} requests/min")
    return _assistant


def log_run(record: dict) -> None:
    spark.createDataFrame([record], RUN_SCHEMA).write.format(TABLE_FORMAT).mode("append") \
        .saveAsTable(tbl("gold", RUNS_TABLE))


def ask(question: str, log: bool = True) -> dict:
    assistant = get_assistant()
    started = time.time()
    state, status, error = {}, "ok", None
    try:
        state = assistant.invoke({"question": question})
    except Exception as exc:
        status, error = "error", f"{type(exc).__name__}: {str(exc)[:1000]}"
    trace = state.get("trace", [])
    tools = [t for t in trace if t["type"] == "tool"]
    used = sorted({t["model"] for t in trace if t.get("model")})
    result = {
        "run_id": str(uuid.uuid4()),
        "run_at": datetime.now(timezone.utc),
        "model": ",".join(used) or _model_name,
        "question": question,
        "route": ",".join(state.get("route", [])),
        "answer": state.get("answer"),
        "sql_queries": [t["content"] for t in tools if t["agent"] == "sql_analyst"],
        "cypher_queries": [t["content"] for t in tools if t["agent"] == "graph_analyst"],
        "blocked_queries": sum(t["status"] == "blocked" for t in tools),
        "failed_queries": sum(t["status"] == "error" for t in tools),
        "input_tokens": sum(t.get("input_tokens", 0) for t in trace),
        "output_tokens": sum(t.get("output_tokens", 0) for t in trace),
        "latency_seconds": round(time.time() - started, 2),
        "status": status,
        "error": error,
        "trace": json.dumps(trace, ensure_ascii=False, default=str),
    }
    if log:
        log_run(result)
    return result


def show(result: dict) -> None:
    print(f"Q: {result['question']}\nRoute: {result['route'] or '(none)'} - {result['latency_seconds']}s - "
          f"tokens in/out {result['input_tokens']:,}/{result['output_tokens']:,}")
    for q in result["sql_queries"]:
        print(f"\n[SQL]\n{q}")
    for q in result["cypher_queries"]:
        print(f"\n[Cypher]\n{q}")
    print(f"\n{result['answer'] or result['error']}\n" + "-" * 80)


print(f"Allowed tables for the SQL analyst: {', '.join(ALLOWED_TABLES)}")
