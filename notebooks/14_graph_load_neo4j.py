# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Graph - Load into Neo4j
# MAGIC
# MAGIC Publishes the municipality graph to Neo4j (AuraDB), so it can be explored visually and queried in Cypher, and later used by the AI agents.
# MAGIC
# MAGIC **Model**
# MAGIC ```
# MAGIC (:PSI:Municipality {psi_key, code, name, population, cvli_rate_100k, lisa_cluster, ...})
# MAGIC     -[:BORDERS {border_km}]-  (:PSI:Municipality)
# MAGIC     -[:IN_STATE]->            (:PSI:State {psi_key, code, name, region})
# MAGIC     -[:IN_COMMUNITY]->        (:PSI:Community {psi_key, id, municipalities, cvli_rate_100k})
# MAGIC ```
# MAGIC `BORDERS` is stored once per pair and queried without direction: `(a)-[:BORDERS]-(b)`.
# MAGIC
# MAGIC **Project isolation.** AuraDB Free has a single database, which other projects may share. Every node of this project therefore carries the extra label `NEO4J_PROJECT_LABEL` (`PSI`) and a unique key `psi_key` (`Municipality:3550308`, `State:SP`, `Community:12`):
# MAGIC - the uniqueness constraint is on `(:PSI {psi_key})`, so it does not collide with a `Municipality` label used by another project;
# MAGIC - the refresh deletes only stale `:PSI` nodes;
# MAGIC - every query in this project matches `:PSI` first.
# MAGIC
# MAGIC **Load strategy (incremental, mark and sweep):** every node and `BORDERS` relationship is upserted with `MERGE` and stamped with the `load_id` of this run; afterwards, only the `:PSI` nodes and borders that were **not** stamped (municipalities or borders that no longer exist, communities that disappeared) are removed, in batches. The graph is never empty during a refresh, and nothing outside `:PSI` is touched. The notebook runs only when the Gold spatial tables, the analytics or the dimensions changed (`run_or_exit`). Counts in Neo4j are checked against the Gold tables at the end.
# MAGIC
# MAGIC ⚠️ A stale `:PSI` node is removed with all its relationships, including relationships that other projects may have created towards it.
# MAGIC
# MAGIC Credentials come from the secret scope `NEO4J_SECRET_SCOPE` (keys `neo4j-uri`, `neo4j-user`, `neo4j-password`).

# COMMAND ----------

# MAGIC %pip install neo4j

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "14_graph_load_neo4j"
run_or_exit(CONSUMER, ["gold_spatial", "gold_municipality_analytics", "gold_dimensions"])

# COMMAND ----------

import uuid

import numpy as np
from neo4j import GraphDatabase

BATCH_SIZE = 1000
P = NEO4J_PROJECT_LABEL

uri = dbutils.secrets.get(NEO4J_SECRET_SCOPE, "neo4j-uri")
auth = (dbutils.secrets.get(NEO4J_SECRET_SCOPE, "neo4j-user"), dbutils.secrets.get(NEO4J_SECRET_SCOPE, "neo4j-password"))


def cypher(query: str) -> str:
    """Injects the project label. Labels cannot be query parameters in Cypher, so the value comes
    only from configuration (never from data) and is validated here."""
    if not P.isidentifier():
        raise ValueError(f"Invalid project label: {P!r}")
    return query.replace("__P__", P)


def to_neo4j(value):
    """Neo4j accepts neither NaN nor numpy scalars: NaN/NA -> None (property not set), numpy -> Python."""
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def clean(records):
    return [{k: to_neo4j(v) for k, v in r.items()} for r in records]


def run_batches(driver, query, rows, label, **params):
    for start in range(0, len(rows), BATCH_SIZE):
        driver.execute_query(cypher(query), rows=rows[start:start + BATCH_SIZE], **params)
    print(f"{label}: {len(rows):,} upserted")


def sweep(driver, query, label, **params):
    """Deletes in batches what the query matches (keeps transactions small on AuraDB Free)."""
    removed = 0
    while True:
        batch = driver.execute_query(cypher(query), **params).records[0]["removed"]
        removed += batch
        if batch == 0:
            break
    print(f"{label}: {removed:,} removed")


municipalities = (spark.table(tbl("gold", "municipality_analytics"))
                  .join(spark.table(tbl("gold", "municipality_spatial_metrics")), "municipality_code", "left")
                  .select(F.col("municipality_code").alias("code"), F.col("municipality_name").alias("name"),
                          "state_code", "region_name", "population", "cvli_victims", "cvli_rate_100k",
                          "cvli_z_score_state", "cvli_z_score_national", "is_state_hotspot", "is_national_hotspot",
                          "ivsc_score", "idhm", "gdp_per_capita_brl", "economic_tier", "fleet_total",
                          "centroid_lat", "centroid_lon", "area_km2", "neighbor_count",
                          "neighbor_avg_cvli_rate_100k", "neighbor_national_hotspots",
                          "lisa_cluster", "lisa_p_value", "eb_rate_std", "degree", "betweenness", "community_id")
                  .toPandas())
states = spark.table(tbl("gold", "dim_state")).toPandas()
communities = spark.table(tbl("gold", "graph_communities")).toPandas()
borders = spark.table(tbl("gold", "municipality_borders")).toPandas()

municipality_rows = clean(municipalities.to_dict("records"))
state_rows = clean(states.to_dict("records"))
community_rows = clean(communities.to_dict("records"))
border_rows = clean(borders.to_dict("records"))

# COMMAND ----------

with GraphDatabase.driver(uri, auth=auth) as driver:
    driver.verify_connectivity()

    # Uniqueness on the project key (idempotent); it also backs every MERGE/MATCH below with an index
    driver.execute_query(cypher(
        "CREATE CONSTRAINT __P___key IF NOT EXISTS FOR (n:__P__) REQUIRE n.psi_key IS UNIQUE"))

    load_id = str(uuid.uuid4())   # stamp of this run: anything without it at the end is stale
    print(f"load_id {load_id}")

    run_batches(driver, """
        UNWIND $rows AS r
        MERGE (s:__P__ {psi_key: 'State:' + r.state_code})
        SET s:State, s.code = r.state_code, s.name = r.state_name, s.region = r.region_name, s.load_id = $load_id
    """, state_rows, "States", load_id=load_id)

    run_batches(driver, """
        UNWIND $rows AS r
        MERGE (c:__P__ {psi_key: 'Community:' + toString(r.community_id)})
        SET c:Community, c.id = r.community_id, c.municipalities = r.municipalities, c.states = r.states,
            c.population = r.population, c.cvli_victims = r.cvli_victims, c.cvli_rate_100k = r.cvli_rate_100k,
            c.load_id = $load_id
    """, community_rows, "Communities", load_id=load_id)

    # State and community links are rebuilt for each municipality, since its community can change
    run_batches(driver, """
        UNWIND $rows AS r
        MERGE (m:__P__ {psi_key: 'Municipality:' + r.code})
        SET m:Municipality, m += r, m.load_id = $load_id
        REMOVE m.state_code, m.region_name, m.community_id
        WITH m, r
        OPTIONAL MATCH (m)-[old:IN_STATE|IN_COMMUNITY]->()
        DELETE old
        WITH DISTINCT m, r
        MATCH (s:__P__ {psi_key: 'State:' + r.state_code})
        MERGE (m)-[:IN_STATE]->(s)
        WITH m, r
        MATCH (c:__P__ {psi_key: 'Community:' + toString(r.community_id)})
        MERGE (m)-[:IN_COMMUNITY]->(c)
    """, municipality_rows, "Municipalities", load_id=load_id)

    run_batches(driver, """
        UNWIND $rows AS r
        MATCH (a:__P__ {psi_key: 'Municipality:' + r.municipality_code_a})
        MATCH (b:__P__ {psi_key: 'Municipality:' + r.municipality_code_b})
        MERGE (a)-[e:BORDERS]-(b)
        SET e.border_km = r.border_km, e.load_id = $load_id
    """, border_rows, "Borders", load_id=load_id)

    # Sweep: only this project's elements that were not stamped by this run
    sweep(driver, """
        MATCH (:__P__:Municipality)-[e:BORDERS]-(:__P__:Municipality)
        WHERE e.load_id IS NULL OR e.load_id <> $load_id
        WITH DISTINCT e LIMIT 5000 DELETE e RETURN count(*) AS removed
    """, "Stale borders", load_id=load_id)
    sweep(driver, """
        MATCH (n:__P__) WHERE n.load_id IS NULL OR n.load_id <> $load_id
        WITH n LIMIT 5000 DETACH DELETE n RETURN count(*) AS removed
    """, "Stale nodes", load_id=load_id)

    # Reconciliation with the Gold tables
    counts = driver.execute_query(cypher("""
        RETURN COUNT { (:__P__:Municipality) } AS municipalities,
               COUNT { (:__P__:State) } AS states,
               COUNT { (:__P__:Community) } AS communities,
               COUNT { (:__P__:Municipality)-[:BORDERS]->(:__P__:Municipality) } AS borders,
               COUNT { (:__P__:Municipality)-[:IN_STATE]->(:__P__:State) } AS in_state
    """)).records[0]
    expected = {"municipalities": len(municipality_rows), "states": len(state_rows),
                "communities": len(community_rows), "borders": len(border_rows),
                "in_state": len(municipality_rows)}
    for key, value in expected.items():
        status = "OK " if counts[key] == value else "DIFF"
        print(f"{status} {key}: Neo4j {counts[key]:,} | Gold {value:,}")
    if any(counts[k] != v for k, v in expected.items()):
        raise AssertionError("Neo4j counts differ from the Gold tables")

log_change("neo4j_graph", [load_id])
finish_step(CONSUMER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example queries

# COMMAND ----------

examples = {
    "Spillover targets: Low-High municipalities that border national hotspots": """
        MATCH (h:__P__:Municipality {is_national_hotspot: true})-[:BORDERS]-(n:__P__:Municipality {lisa_cluster: 'Low-High'}),
              (n)-[:IN_STATE]->(s:__P__:State)
        RETURN n.name AS municipality, s.code AS state, n.cvli_rate_100k AS rate,
               n.neighbor_avg_cvli_rate_100k AS neighbors_rate, count(DISTINCT h) AS hotspot_neighbors
        ORDER BY hotspot_neighbors DESC, neighbors_rate DESC LIMIT 10""",
    "High-High municipalities next to national hotspots that are not hotspots themselves": """
        MATCH (h:__P__:Municipality {is_national_hotspot: true})-[:BORDERS]-(n:__P__:Municipality {lisa_cluster: 'High-High'}),
              (n)-[:IN_STATE]->(s:__P__:State)
        WHERE NOT n.is_national_hotspot
        RETURN n.name AS municipality, s.code AS state, n.population AS population, n.cvli_rate_100k AS rate,
               count(DISTINCT h) AS hotspot_neighbors
        ORDER BY hotspot_neighbors DESC, rate DESC LIMIT 10""",
    "High-High clusters that cross state borders": """
        MATCH (a:__P__:Municipality {lisa_cluster: 'High-High'})-[:BORDERS]-(b:__P__:Municipality {lisa_cluster: 'High-High'}),
              (a)-[:IN_STATE]->(sa:__P__:State), (b)-[:IN_STATE]->(sb:__P__:State)
        WHERE sa.code < sb.code
        RETURN sa.code AS state_a, a.name AS municipality_a, sb.code AS state_b, b.name AS municipality_b
        LIMIT 10""",
    "Communities with the highest CVLI rate": """
        MATCH (c:__P__:Community) WHERE c.municipalities >= 5
        RETURN c.id AS community, c.municipalities AS municipalities, c.states AS states, c.cvli_rate_100k AS rate
        ORDER BY rate DESC LIMIT 10""",
}
with GraphDatabase.driver(uri, auth=auth) as driver:
    for title, query in examples.items():
        print(f"\n{title}")
        for record in driver.execute_query(cypher(query)).records:
            print("  ", dict(record))
