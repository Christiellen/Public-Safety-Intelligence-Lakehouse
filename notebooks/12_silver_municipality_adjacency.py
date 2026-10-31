# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Silver - Municipality adjacency (who borders whom)
# MAGIC
# MAGIC Turns the official municipal mesh into a **graph of neighbors**: two municipalities are adjacent when they share a stretch of border (*rook* contiguity). Contacts at a single point do not count, because a shared vertex does not connect two territories in practice.
# MAGIC
# MAGIC **Method**
# MAGIC 1. Read the zip recorded in `bronze_ibge_mesh_files`; repair invalid geometries.
# MAGIC 2. Project to SIRGAS 2000 / Brazil Polyconic (`PROJECTED_CRS`) so lengths and areas come out in metres.
# MAGIC 3. Candidate pairs from a spatial index (`sjoin` with `intersects`), then the length of the shared boundary for each pair. Pairs with length 0 are point contacts and are dropped (and counted).
# MAGIC
# MAGIC **Outputs**
# MAGIC - `municipality_adjacency`: one row per pair (`municipality_code_a < municipality_code_b`), with `border_km`.
# MAGIC - `municipality_geo`: centroid (lat/lon), area and number of neighbors per municipality. Municipalities with no neighbor (e.g. ocean islands such as Fernando de Noronha) are kept, with `neighbor_count = 0`.
# MAGIC
# MAGIC Every code in the mesh must exist in `dim_municipality`, and vice versa; differences are reported.
# MAGIC
# MAGIC **Incremental:** Recomputed only when a new mesh edition was loaded or the municipality dimension changed (borders change about once a year).

# COMMAND ----------

# MAGIC %pip install geopandas pyogrio

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "12_silver_municipality_adjacency"
pending = run_or_exit(CONSUMER, ["bronze_ibge_mesh", "silver_dim_municipality"],
                      outputs=[("silver", "municipality_adjacency"), ("silver", "municipality_geo")])

# COMMAND ----------

import shutil

import geopandas as gpd
import numpy as np
import shapely

manifest = spark.table(tbl("bronze", "bronze_ibge_mesh_files")).orderBy(F.desc("_ingested_at")).first()
local_zip = f"/tmp/{manifest.file_name}"
shutil.copy(manifest.volume_path, local_zip)
print(f"Mesh {manifest.mesh_year}: {manifest.file_name}")

mesh = gpd.read_file(local_zip)
code_column = next((c for c in mesh.columns if c.upper() == "CD_MUN"), None)
if code_column is None:
    raise ValueError(f"CD_MUN column not found. Columns: {list(mesh.columns)}")

mesh = mesh[[code_column, "geometry"]].rename(columns={code_column: "municipality_code"})
mesh["municipality_code"] = mesh["municipality_code"].astype(str).str.zfill(7)
mesh["geometry"] = shapely.make_valid(mesh.geometry.values)
if mesh["municipality_code"].duplicated().any():                 # one geometry per code
    mesh = mesh.dissolve(by="municipality_code", as_index=False)
print(f"Municipalities in the mesh: {len(mesh):,}")

# COMMAND ----------

# Codes must match the conformed dimension
dim_codes = {r.municipality_code for r in spark.table(tbl("silver", "dim_municipality")).select("municipality_code").collect()}
mesh_codes = set(mesh["municipality_code"])
only_mesh, only_dim = sorted(mesh_codes - dim_codes), sorted(dim_codes - mesh_codes)
print(f"In the mesh but not in dim_municipality: {len(only_mesh)} {only_mesh[:20]}")
print(f"In dim_municipality but not in the mesh: {len(only_dim)} {only_dim[:20]}")
mesh = mesh[mesh["municipality_code"].isin(dim_codes)]

projected = mesh.to_crs(PROJECTED_CRS)

# Candidate pairs from the spatial index, each pair once
pairs = gpd.sjoin(projected, projected, predicate="intersects", how="inner", lsuffix="a", rsuffix="b")
pairs = pairs[pairs["municipality_code_a"] < pairs["municipality_code_b"]][["municipality_code_a", "municipality_code_b"]]

geometry_by_code = projected.set_index("municipality_code").geometry
boundary_a = shapely.boundary(geometry_by_code.loc[pairs["municipality_code_a"]].values)
boundary_b = shapely.boundary(geometry_by_code.loc[pairs["municipality_code_b"]].values)
pairs["border_km"] = np.round(shapely.length(shapely.intersection(boundary_a, boundary_b)) / 1000, 3)

point_contacts = int((pairs["border_km"] == 0).sum())
adjacency = pairs[pairs["border_km"] > 0].reset_index(drop=True)
print(f"Adjacent pairs: {len(adjacency):,} | point-only contacts dropped: {point_contacts:,}")

# COMMAND ----------

# Per-municipality geography
neighbor_count = (pd.concat([adjacency["municipality_code_a"], adjacency["municipality_code_b"]])
                  .value_counts().rename("neighbor_count"))
centroids = projected.geometry.representative_point().to_crs("EPSG:4674")
geo = pd.DataFrame({
    "municipality_code": projected["municipality_code"].values,
    "centroid_lat": np.round(centroids.y.values, 6),
    "centroid_lon": np.round(centroids.x.values, 6),
    "area_km2": np.round(projected.geometry.area.values / 1e6, 3),
}).merge(neighbor_count, left_on="municipality_code", right_index=True, how="left")
geo["neighbor_count"] = geo["neighbor_count"].fillna(0).astype(int)
geo["mesh_year"] = int(manifest.mesh_year)

isolated = geo[geo["neighbor_count"] == 0]["municipality_code"].tolist()
print(f"Municipalities without neighbors: {len(isolated)} {isolated}")
print(f"Neighbors per municipality: median {geo['neighbor_count'].median():.0f}, max {geo['neighbor_count'].max()}")

df_adjacency = spark.createDataFrame(adjacency.assign(mesh_year=int(manifest.mesh_year)))
df_geo = nan_to_null(spark.createDataFrame(geo))
assert_unique(df_adjacency, ["municipality_code_a", "municipality_code_b"], "municipality_adjacency")
assert_unique(df_geo, ["municipality_code"], "municipality_geo")
write_table(df_adjacency, "silver", "municipality_adjacency")
write_table(df_geo, "silver", "municipality_geo")
log_change("silver_adjacency", ["all"])
finish_step(CONSUMER)
