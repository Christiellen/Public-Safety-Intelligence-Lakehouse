# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Gold - Municipality analytics
# MAGIC
# MAGIC One row per municipality with comparable, documented metrics.
# MAGIC
# MAGIC **Main indicator: CVLI rate.** CVLI (*Crimes Violentos Letais Intencionais*) is the standard Brazilian indicator of lethal violence: victims of intentional homicide, feminicide, robbery followed by death (*latrocínio*) and bodily injury followed by death (list in `CVLI_EVENTS`). Feminicide is published as a separate event, not inside intentional homicide, so adding them does not double count (evidence in `00_config_utils`). It is published by SINESP for every municipality, including months with zero victims, so a zero here is a real zero.
# MAGIC
# MAGIC **Window.** The last `WINDOW_MONTHS` months available. Rates are **annualized** (`× 12 / months_covered`) so a partial year does not look safer than a full one.
# MAGIC
# MAGIC **Hotspots (Z-Score).** Calculated on the **rate**, not on the absolute count, otherwise large cities would always stand out. Only municipalities with at least `MIN_POPULATION` inhabitants enter the reference mean and standard deviation, and only they receive a score, because very small populations produce unstable rates (one homicide in a town of 3,000 people is a rate of 33 per 100k). Two views, side by side:
# MAGIC - `cvli_z_score_state` / `is_state_hotspot`: compared with the municipalities of the **same state**. Highlights cities that stand out in their own context, even where the state average is low.
# MAGIC - `cvli_z_score_national` / `is_national_hotspot`: compared with **all eligible municipalities in Brazil**. Highlights the highest absolute levels of lethal violence.
# MAGIC
# MAGIC A municipality is flagged when `z ≥ HOTSPOT_Z`.
# MAGIC
# MAGIC **IVSC (Social & Criminal Vulnerability Index).** `0.4 × crime component + 0.6 × social component`, both rescaled to 0-1 before weighting:
# MAGIC - crime component: CVLI rate, capped at the national 1st and 99th percentiles, then min-max scaled;
# MAGIC - social component: `1 - IDHM`, min-max scaled.
# MAGIC
# MAGIC Without rescaling, a rate in the tens would dominate an index in the 0-1 range. IDHM refers to 2010 (latest municipal edition).
# MAGIC
# MAGIC **Economic tier.** National tertiles of GDP per capita.
# MAGIC
# MAGIC Vehicle crimes are published by SINESP at state level only, so they are analysed in `10_gold_state_analytics`.
# MAGIC
# MAGIC **Incremental:** Recomputed only when the Gold facts or dimensions changed. The window moves with the data, so the table is rebuilt (5,570 rows) rather than merged. A change of `WINDOW_MONTHS`, `MIN_POPULATION` or `HOTSPOT_Z` needs `FULL_REFRESH`.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

CONSUMER = "09_gold_municipality_analytics"
run_or_exit(CONSUMER, ["gold_facts", "gold_dimensions"],
            outputs=[("gold", "municipality_analytics")])

# COMMAND ----------

WINDOW_MONTHS = 12
MIN_POPULATION = 20000
HOTSPOT_Z = 2.0
IVSC_WEIGHTS = {"crime": 0.4, "social": 0.6}

fact = spark.table(tbl("gold", "fact_occurrences"))
dim_event = spark.table(tbl("gold", "dim_event"))
dim_date = spark.table(tbl("gold", "dim_date"))
dim_municipality = spark.table(tbl("gold", "dim_municipality"))
indicators = spark.table(tbl("gold", "fact_municipality_indicators"))

cvli_facts = (fact.filter(F.col("measure_type") == "victims")
              .join(dim_event.filter("is_cvli").select("event_id"), "event_id"))

# Window over the months that actually have CVLI data
cvli_dates = dim_date.join(cvli_facts.select("date_id").distinct(), "date_id")
last_date = cvli_dates.agg(F.max("date")).first()[0]
window_dates = cvli_dates.filter(F.col("date") > F.add_months(F.lit(last_date), -WINDOW_MONTHS))
months_covered = window_dates.count()
first_date = window_dates.agg(F.min("date")).first()[0]
print(f"Window: {first_date} to {last_date} ({months_covered} months)")

# COMMAND ----------

cvli = (cvli_facts.join(window_dates.select("date_id"), "date_id")
        .groupBy("municipality_code")
        .agg(F.sum("count").alias("cvli_victims")))

annualize = F.lit(12.0 / months_covered)
base = (dim_municipality.select("municipality_code", "municipality_name", "state_code", "region_name")
        .join(indicators, "municipality_code", "left")
        .join(cvli, "municipality_code", "left")
        # No SINESP record in the window means "no data", not "zero": the rate stays null
        .withColumn("has_sinesp_records", F.col("cvli_victims").isNotNull())
        .withColumn("months_covered", F.lit(months_covered))
        .withColumn("window_start", F.lit(first_date))
        .withColumn("window_end", F.lit(last_date))
        .withColumn("cvli_rate_100k",
                    F.when(F.col("population") > 0,
                           F.round(F.col("cvli_victims") * annualize / F.col("population") * 100000, 2))))

# COMMAND ----------

# Z-Scores, using only municipalities with stable rates as the reference
eligible = (F.col("population") >= MIN_POPULATION) & F.col("cvli_rate_100k").isNotNull()

# Within state
state_stats = (base.filter(eligible).groupBy("state_code")
               .agg(F.avg("cvli_rate_100k").alias("_state_mean"),
                    F.stddev("cvli_rate_100k").alias("_state_std")))

# National
national = base.filter(eligible).agg(F.avg("cvli_rate_100k").alias("mean"),
                                     F.stddev("cvli_rate_100k").alias("std")).first()
print(f"National reference (municipalities with {MIN_POPULATION:,}+ inhabitants): "
      f"mean {national['mean']:.2f}, std {national['std']:.2f} CVLI per 100k")

analytics = (base.join(state_stats, "state_code", "left")
             .withColumn("cvli_z_score_state",
                         F.when(eligible & (F.col("_state_std") > 0),
                                F.round((F.col("cvli_rate_100k") - F.col("_state_mean")) / F.col("_state_std"), 2)))
             .withColumn("cvli_z_score_national",
                         F.when(eligible & F.lit(national["std"] is not None and national["std"] > 0),
                                F.round((F.col("cvli_rate_100k") - F.lit(national["mean"])) / F.lit(national["std"]), 2)))
             .withColumn("is_state_hotspot", F.coalesce(F.col("cvli_z_score_state") >= HOTSPOT_Z, F.lit(False)))
             .withColumn("is_national_hotspot", F.coalesce(F.col("cvli_z_score_national") >= HOTSPOT_Z, F.lit(False)))
             .drop("_state_mean", "_state_std"))

# IVSC: winsorize the rate at national p1/p99, then min-max both components
quantiles = analytics.approxQuantile("cvli_rate_100k", [0.01, 0.99], 0.001)
if len(quantiles) < 2:
    raise AssertionError("No CVLI rates available to build the IVSC")
p01, p99 = quantiles
# greatest/least ignore nulls, so a missing rate must stay null explicitly
rate_capped = F.when(F.col("cvli_rate_100k").isNotNull(),
                     F.least(F.greatest(F.col("cvli_rate_100k"), F.lit(p01)), F.lit(p99)))
analytics = analytics.withColumn("_rate_capped", rate_capped).withColumn("_social", 1 - F.col("idhm"))

bounds = analytics.agg(F.min("_rate_capped").alias("rmin"), F.max("_rate_capped").alias("rmax"),
                       F.min("_social").alias("smin"), F.max("_social").alias("smax")).first()


def min_max(col, low, high):
    return F.when(F.lit(high) > F.lit(low), (col - F.lit(low)) / (F.lit(high) - F.lit(low)))


analytics = (analytics
             .withColumn("crime_component", F.round(min_max(F.col("_rate_capped"), bounds.rmin, bounds.rmax), 4))
             .withColumn("social_component", F.round(min_max(F.col("_social"), bounds.smin, bounds.smax), 4))
             .withColumn("ivsc_score", F.round(IVSC_WEIGHTS["crime"] * F.col("crime_component") +
                                               IVSC_WEIGHTS["social"] * F.col("social_component"), 4))
             .drop("_rate_capped", "_social"))

# Economic tier by national tertiles of GDP per capita
tier_window = Window.orderBy("gdp_per_capita_brl")
with_gdp = (analytics.filter(F.col("gdp_per_capita_brl").isNotNull())
            .withColumn("_tertile", F.ntile(3).over(tier_window))
            .select("municipality_code", "_tertile"))
analytics = (analytics.join(with_gdp, "municipality_code", "left")
             .withColumn("economic_tier", F.when(F.col("_tertile") == 3, "High")
                                           .when(F.col("_tertile") == 2, "Medium")
                                           .when(F.col("_tertile") == 1, "Low"))
             .drop("_tertile"))

# COMMAND ----------

assert_unique(analytics, ["municipality_code"], "gold.municipality_analytics")
write_table(analytics, "gold", "municipality_analytics")
log_change("gold_municipality_analytics", ["all"])
finish_step(CONSUMER)

result = spark.table(tbl("gold", "municipality_analytics"))
result.agg(
    F.count(F.lit(1)).alias("municipalities"),
    F.count("cvli_rate_100k").alias("with_rate"),
    F.count("ivsc_score").alias("with_ivsc"),
    F.sum(F.col("is_state_hotspot").cast("int")).alias("state_hotspots"),
    F.sum(F.col("is_national_hotspot").cast("int")).alias("national_hotspots"),
    F.sum((F.col("is_state_hotspot") & F.col("is_national_hotspot")).cast("int")).alias("both")).show()

display(result
        .filter("is_state_hotspot OR is_national_hotspot")
        .select("municipality_name", "state_code", "population", "cvli_victims", "cvli_rate_100k",
                "cvli_z_score_state", "cvli_z_score_national", "is_state_hotspot", "is_national_hotspot",
                "ivsc_score", "economic_tier")
        .orderBy(F.desc("cvli_rate_100k")))
