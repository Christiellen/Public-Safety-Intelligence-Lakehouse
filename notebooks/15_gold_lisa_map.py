# Databricks notebook source
# MAGIC %md
# MAGIC # 15 - Gold - LISA cluster map
# MAGIC
# MAGIC The standard map for local spatial autocorrelation: every municipality colored by its LISA cluster (`13_gold_spatial_analytics`), with state borders on top.
# MAGIC
# MAGIC | Cluster | Color | Meaning |
# MAGIC |---|---|---|
# MAGIC | High-High | red | High CVLI rate surrounded by high rates (regional cluster) |
# MAGIC | Low-Low | blue | Low rate surrounded by low rates |
# MAGIC | High-Low | light red | High rate surrounded by low rates (isolated outlier) |
# MAGIC | Low-High | light blue | Low rate surrounded by high rates (spillover target) |
# MAGIC | Not significant | light grey | No significant local pattern at 5% |
# MAGIC | No neighbors / No data | dark grey | Islands, or municipality without rate |
# MAGIC
# MAGIC Boundaries are simplified for drawing only (`SIMPLIFY_METERS`); the analysis in notebooks 12 and 13 uses the full-resolution mesh. State outlines are dissolved from the full-resolution mesh before simplification, so no gaps appear inside the states. The PNG is saved to the volume `reports` in the Gold schema.

# COMMAND ----------

# MAGIC %pip install geopandas pyogrio matplotlib

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

import os
import shutil

import geopandas as gpd
import matplotlib.pyplot as plt
import shapely
from matplotlib.patches import Patch

SIMPLIFY_METERS = 500
DPI = 200
COLORS = {
    "High-High": "#d7191c",
    "Low-Low": "#2c7bb6",
    "High-Low": "#fdae61",
    "Low-High": "#abd9e9",
    "Not significant": "#eeeeee",
    "No neighbors": "#9e9e9e",
    "No data": "#9e9e9e",
}

# Mesh (same file as notebook 12)
manifest = spark.table(tbl("bronze", "bronze_ibge_mesh_files")).orderBy(F.desc("_ingested_at")).first()
local_zip = f"/tmp/{manifest.file_name}"
shutil.copy(manifest.volume_path, local_zip)
mesh = gpd.read_file(local_zip)
code_column = next(c for c in mesh.columns if c.upper() == "CD_MUN")
mesh = mesh[[code_column, "geometry"]].rename(columns={code_column: "municipality_code"})
mesh["municipality_code"] = mesh["municipality_code"].astype(str).str.zfill(7)
mesh = mesh.to_crs(PROJECTED_CRS)
mesh["geometry"] = shapely.make_valid(mesh.geometry.values)

# State outlines from the FULL-resolution mesh, simplified only after dissolving. Simplifying each
# municipality first opens tiny gaps between neighbors, which would show up as stray lines.
states = mesh.assign(state=mesh["municipality_code"].str[:2]).dissolve(by="state")
states["geometry"] = states.geometry.simplify(SIMPLIFY_METERS, preserve_topology=True)

# Municipalities simplified for drawing only
mesh["geometry"] = shapely.make_valid(mesh.geometry.simplify(SIMPLIFY_METERS, preserve_topology=True).values)

# LISA clusters and the global statistic
metrics = spark.table(tbl("gold", "municipality_spatial_metrics")).select(
    "municipality_code", "lisa_cluster", "global_moran_i", "global_moran_p_value").toPandas()
moran_i, moran_p = metrics["global_moran_i"].iloc[0], metrics["global_moran_p_value"].iloc[0]

data = mesh.merge(metrics[["municipality_code", "lisa_cluster"]], on="municipality_code", how="inner")
missing = set(metrics["municipality_code"]) - set(data["municipality_code"])
print(f"Municipalities drawn: {len(data):,} | without geometry: {len(missing)}")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 12))
for cluster, color in COLORS.items():
    subset = data[data["lisa_cluster"] == cluster]
    if len(subset):
        # Thin edge in the fill color covers the hairline gaps left by simplification
        subset.plot(ax=ax, color=color, edgecolor=color, linewidth=0.2)
states.boundary.plot(ax=ax, color="#424242", linewidth=0.6)

counts = data["lisa_cluster"].value_counts()
legend_order = ["High-High", "Low-Low", "High-Low", "Low-High", "Not significant", "No neighbors", "No data"]
handles = [Patch(facecolor=COLORS[c], edgecolor="#757575", label=f"{c} ({counts.get(c, 0):,})")
           for c in legend_order if counts.get(c, 0)]
ax.legend(handles=handles, loc="lower left", frameon=False, fontsize=11, title="LISA cluster (5%)", title_fontsize=11)

window = spark.table(tbl("gold", "municipality_analytics")).select("window_start", "window_end").first()
ax.set_title(f"Lethal intentional violence (CVLI) — LISA clusters by municipality\n"
             f"{window.window_start:%m/%Y} to {window.window_end:%m/%Y} · Global Moran's I (EB rate) = "
             f"{moran_i:.3f}, pseudo p = {moran_p:.3f}", fontsize=13, loc="left")
ax.text(0.99, 0.01, "Sources: SINESP/MJSP, IBGE. Empirical Bayes rates, 999 permutations.",
        transform=ax.transAxes, ha="right", va="bottom", fontsize=8, color="#616161")
ax.set_axis_off()
plt.tight_layout()

# Save to the reports volume (and keep a local copy for display)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS['gold']}.reports")
local_png = "/tmp/lisa_cluster_map.png"
fig.savefig(local_png, dpi=DPI, bbox_inches="tight", facecolor="white")
target = f"/Volumes/{CATALOG}/{SCHEMAS['gold']}/reports/lisa_cluster_map.png"
shutil.copy(local_png, target)
print(f"Saved {target}")
plt.show()