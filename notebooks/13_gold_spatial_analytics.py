# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Gold - Spatial and graph analytics
# MAGIC
# MAGIC Answers one question with statistics instead of maps alone: **does lethal violence spill over between neighboring municipalities?**
# MAGIC
# MAGIC **1. Global Moran's I (rate version).** Measures whether municipalities with high CVLI rates tend to border other high-rate municipalities. The *rate* version (`esda.Moran_Rate`) applies an Empirical Bayes standardization: a town of 3,000 people with one homicide does not weigh like a city of 300,000. Significance comes from 999 random permutations.
# MAGIC
# MAGIC **2. Local Moran (LISA).** Classifies each municipality, at 5% significance:
# MAGIC - `High-High`: high rate surrounded by high rates, a regional violence cluster;
# MAGIC - `Low-Low`: low rate surrounded by low rates;
# MAGIC - `High-Low`: high rate surrounded by low rates, an isolated outlier;
# MAGIC - `Low-High`: low rate surrounded by high rates, a likely spillover target;
# MAGIC - `Not significant`; `No neighbors` for islands; `No data` without rate.
# MAGIC
# MAGIC **3. Neighborhood metrics.** Average CVLI rate of the neighbors (spatial lag) and number of neighbors that are national hotspots.
# MAGIC
# MAGIC **4. Graph structure (NetworkX).**
# MAGIC - *Communities (Louvain)*: groups of contiguous municipalities with similar violence levels. Edge weight = similarity of the Empirical Bayes rates, so communities follow the data, not only the map.
# MAGIC - *Betweenness centrality*: municipalities that act as bridges between regions of the graph (approximated with a fixed sample for speed and reproducibility).
# MAGIC
# MAGIC All random steps use `SEED`, so results are reproducible.
# MAGIC
# MAGIC **Incremental:** Recomputed only when the adjacency or the municipality analytics changed.

# COMMAND ----------

# MAGIC %pip install esda libpysal networkx

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "13_gold_spatial_analytics"
run_or_exit(CONSUMER, ["silver_adjacency", "gold_municipality_analytics"],
            outputs=[("gold", "municipality_spatial_metrics"), ("gold", "graph_communities"),
                     ("gold", "municipality_borders")])

# COMMAND ----------

import warnings

import esda
import networkx as nx
import numpy as np
from libpysal.weights import W

SEED = 42
PERMUTATIONS = 999
SIGNIFICANCE = 0.05
BETWEENNESS_SAMPLE = 1000

analytics = spark.table(tbl("gold", "municipality_analytics")).select(
    "municipality_code", "municipality_name", "state_code", "population", "cvli_victims",
    "cvli_rate_100k", "is_national_hotspot").toPandas()
adjacency = spark.table(tbl("silver", "municipality_adjacency")).toPandas()
geo = spark.table(tbl("silver", "municipality_geo")).toPandas()

neighbors = {code: [] for code in analytics["municipality_code"]}
for a, b in zip(adjacency["municipality_code_a"], adjacency["municipality_code_b"], strict=True):
    if a in neighbors and b in neighbors:
        neighbors[a].append(b)
        neighbors[b].append(a)
print(f"Municipalities: {len(neighbors):,} | adjacent pairs: {len(adjacency):,}")

# COMMAND ----------

# 1-2. Moran's I (global and local), on municipalities with data and at least one neighbor with data
has_data = analytics["cvli_victims"].notna() & (analytics["population"] > 0)
data_codes = set(analytics.loc[has_data, "municipality_code"])
study_neighbors = {c: [n for n in neighbors[c] if n in data_codes] for c in data_codes}
study_codes = sorted(c for c, ns in study_neighbors.items() if ns)
study = analytics.set_index("municipality_code").loc[study_codes]

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    weights = W({c: study_neighbors[c] for c in study_codes}, id_order=study_codes, silence_warnings=True)
    events = study["cvli_victims"].astype(float).values
    base = study["population"].astype(float).values
    moran = esda.Moran_Rate(events, base, weights, permutations=PERMUTATIONS)
    lisa = esda.Moran_Local_Rate(events, base, weights, permutations=PERMUTATIONS, seed=SEED)

print(f"Global Moran's I (EB rate) = {moran.I:.3f} | pseudo p-value = {moran.p_sim:.3f} | "
      f"{'significant spatial clustering' if moran.p_sim < SIGNIFICANCE else 'no significant clustering'}")

quadrant = {1: "High-High", 2: "Low-High", 3: "Low-Low", 4: "High-Low"}
lisa_table = pd.DataFrame({
    "municipality_code": study_codes,
    "lisa_z_sim": np.round(lisa.z_sim, 3),
    "lisa_i": np.round(lisa.Is, 4),
    "lisa_p_value": np.round(lisa.p_sim, 4),
    "lisa_cluster": [quadrant[q] if p < SIGNIFICANCE else "Not significant" for q, p in zip(lisa.q, lisa.p_sim, strict=True)],
})

# Empirical Bayes standardized rate (Assunção & Reis), the same transformation used by Moran_Rate.
# Reused as the similarity signal for the communities and saved for consumers.
eb_rate = pd.Series(esda.smoothing.assuncao_rate(events, base), index=study_codes, name="eb_rate_std")
lisa_table["eb_rate_std"] = np.round(eb_rate.values, 4)

# COMMAND ----------

# 3. Neighborhood metrics
rate = analytics.set_index("municipality_code")["cvli_rate_100k"]
national = analytics.set_index("municipality_code")["is_national_hotspot"].fillna(False)
neighborhood = pd.DataFrame({
    "municipality_code": list(neighbors),
    "neighbor_avg_cvli_rate_100k": [
        round(float(rate.loc[ns].mean()), 2) if ns and rate.loc[ns].notna().any() else None
        for ns in neighbors.values()],
    "neighbor_national_hotspots": [int(national.loc[ns].sum()) if ns else 0 for ns in neighbors.values()],
})

# 4. Graph: Louvain communities on similarity-weighted edges, and betweenness centrality
graph = nx.Graph()
graph.add_nodes_from(neighbors)
for a, b, km in zip(adjacency["municipality_code_a"], adjacency["municipality_code_b"], adjacency["border_km"], strict=True):
    if a in neighbors and b in neighbors:
        if a in eb_rate.index and b in eb_rate.index:
            similarity = 1.0 / (1.0 + abs(eb_rate[a] - eb_rate[b]))
        else:
            similarity = 0.1   # no data on one side: keep the edge, with little weight
        graph.add_edge(a, b, weight=similarity, border_km=km)

communities = nx.community.louvain_communities(graph, weight="weight", seed=SEED)
community_of = {code: idx for idx, members in enumerate(sorted(communities, key=len, reverse=True)) for code in members}
betweenness = nx.betweenness_centrality(graph, k=min(BETWEENNESS_SAMPLE, graph.number_of_nodes()), seed=SEED)
print(f"Louvain communities: {len(communities):,} | modularity = "
      f"{nx.community.modularity(graph, communities, weight='weight'):.3f}")

structure = pd.DataFrame({
    "municipality_code": list(graph.nodes),
    "degree": [graph.degree(n) for n in graph.nodes],
    "betweenness": [round(betweenness[n], 6) for n in graph.nodes],
    "community_id": [community_of[n] for n in graph.nodes],
})

# COMMAND ----------

# Assemble and save
metrics = (analytics[["municipality_code"]]
           .merge(geo[["municipality_code", "centroid_lat", "centroid_lon", "area_km2", "neighbor_count"]], how="left")
           .merge(neighborhood, how="left")
           .merge(lisa_table, how="left")
           .merge(structure, how="left"))
metrics["lisa_cluster"] = np.select(
    [metrics["lisa_cluster"].notna(), metrics["neighbor_count"].fillna(0) == 0],
    [metrics["lisa_cluster"], "No neighbors"], default="No data")
metrics["global_moran_i"] = round(float(moran.I), 4)
metrics["global_moran_p_value"] = round(float(moran.p_sim), 4)

df_metrics = nan_to_null(spark.createDataFrame(metrics.astype({"degree": "Int64", "community_id": "Int64", "neighbor_count": "Int64"})))
assert_unique(df_metrics, ["municipality_code"], "gold.municipality_spatial_metrics")
write_table(df_metrics, "gold", "municipality_spatial_metrics")

communities_summary = (metrics.merge(analytics[["municipality_code", "state_code", "population", "cvli_victims"]])
                       .groupby("community_id")
                       .agg(municipalities=("municipality_code", "count"),
                            states=("state_code", lambda s: ",".join(sorted(s.unique()))),
                            population=("population", "sum"),
                            cvli_victims=("cvli_victims", "sum"))
                       .reset_index())
communities_summary["cvli_rate_100k"] = np.round(communities_summary["cvli_victims"] / communities_summary["population"] * 1e5, 2)
write_table(nan_to_null(spark.createDataFrame(communities_summary)), "gold", "graph_communities")

borders = adjacency[["municipality_code_a", "municipality_code_b", "border_km"]]
write_table(spark.createDataFrame(borders), "gold", "municipality_borders")
log_change("gold_spatial", ["all"])
finish_step(CONSUMER)

display(spark.table(tbl("gold", "municipality_spatial_metrics")).groupBy("lisa_cluster").count().orderBy(F.desc("count")))
