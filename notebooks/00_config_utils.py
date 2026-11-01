# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuration and Shared Utilities
# MAGIC
# MAGIC Shared settings and helpers for every notebook in this project. Each notebook loads it with `%run ./00_config_utils`.
# MAGIC
# MAGIC **What lives here**
# MAGIC - Catalog/schema layout for the Bronze, Silver and Gold layers.
# MAGIC - Source URLs (IBGE SIDRA, IBGE Localidades, SINESP, SENATRAN, Ipeadata).
# MAGIC - HTTP download with retries and exponential backoff.
# MAGIC - Municipality name normalization and **name + state matching to the official IBGE 7-digit code**, the single join key of the whole lakehouse.
# MAGIC - Data quality helpers (uniqueness assertions, coverage reports).
# MAGIC - **Incremental processing**: a change log of every dataset, so each notebook processes only what changed since its last run (details in the last section).

# COMMAND ----------

import re
import time
import unicodedata

import pandas as pd
import requests
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Storage layout (Unity Catalog). On Databricks Free Edition the default
# catalog is "workspace". Set CATALOG = None to use the legacy metastore.
# ---------------------------------------------------------------------------
CATALOG = "workspace"
SCHEMAS = {"bronze": "psi_bronze", "silver": "psi_silver", "gold": "psi_gold",
           "ops": "psi_ops"}   # ops: change log and processing control of the pipeline
TABLE_FORMAT = "delta"
HTTP_TIMEOUT = (20, 180)   # (connect, read) seconds: unreachable hosts fail fast


def tbl(layer: str, name: str) -> str:
    """Fully qualified table name for a layer."""
    schema = SCHEMAS[layer]
    return f"{CATALOG}.{schema}.{name}" if CATALOG else f"{schema}.{name}"


def ensure_schemas() -> None:
    for schema in SCHEMAS.values():
        target = f"{CATALOG}.{schema}" if CATALOG else schema
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Sources (all public). Update the SENATRAN month and SINESP years as new
# files are published.
# ---------------------------------------------------------------------------
SIDRA_BASE = "https://apisidra.ibge.gov.br/values"
# SIDRA rejects queries above ~100,000 values (HTTP 400): 5,570 municipalities x 5 years stays well below.
SIDRA_POPULATION = {"table": "6579", "variable": "9324", "period": "last%205"}   # Estimated resident population
SIDRA_GDP = {"table": "5938", "variable": "37"}            # GDP at current prices (R$ thousand)

# Optional enrichment (meso/micro/immediate regions). Some compute environments
# cannot reach this host; the pipeline works without it.
IBGE_MUNICIPALITIES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

# Official IBGE state codes (first two digits of the municipality code).
UF_REFERENCE = {
    "11": ("RO", "Rondônia", "N", "Norte"), "12": ("AC", "Acre", "N", "Norte"),
    "13": ("AM", "Amazonas", "N", "Norte"), "14": ("RR", "Roraima", "N", "Norte"),
    "15": ("PA", "Pará", "N", "Norte"), "16": ("AP", "Amapá", "N", "Norte"),
    "17": ("TO", "Tocantins", "N", "Norte"),
    "21": ("MA", "Maranhão", "NE", "Nordeste"), "22": ("PI", "Piauí", "NE", "Nordeste"),
    "23": ("CE", "Ceará", "NE", "Nordeste"), "24": ("RN", "Rio Grande do Norte", "NE", "Nordeste"),
    "25": ("PB", "Paraíba", "NE", "Nordeste"), "26": ("PE", "Pernambuco", "NE", "Nordeste"),
    "27": ("AL", "Alagoas", "NE", "Nordeste"), "28": ("SE", "Sergipe", "NE", "Nordeste"),
    "29": ("BA", "Bahia", "NE", "Nordeste"),
    "31": ("MG", "Minas Gerais", "SE", "Sudeste"), "32": ("ES", "Espírito Santo", "SE", "Sudeste"),
    "33": ("RJ", "Rio de Janeiro", "SE", "Sudeste"), "35": ("SP", "São Paulo", "SE", "Sudeste"),
    "41": ("PR", "Paraná", "S", "Sul"), "42": ("SC", "Santa Catarina", "S", "Sul"),
    "43": ("RS", "Rio Grande do Sul", "S", "Sul"),
    "50": ("MS", "Mato Grosso do Sul", "CO", "Centro-Oeste"), "51": ("MT", "Mato Grosso", "CO", "Centro-Oeste"),
    "52": ("GO", "Goiás", "CO", "Centro-Oeste"), "53": ("DF", "Distrito Federal", "CO", "Centro-Oeste"),
}

SINESP_URL_TEMPLATE = (
    "https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/"
    "download/dnsp-base-de-dados/bancovde-{year}.xlsx/@@download/file"
)
SINESP_YEARS = ["2025", "2026"]

SENATRAN_FLEET_URL = (
    "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/"
    "frota-de-veiculos-2026-2/Frota_por_municipio_e_tipo_Setembro_2026.xlsx/@@download/file"
)
SENATRAN_REFERENCE = "2026-09"

IPEADATA_URL_TEMPLATE = "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{series}')"
IDHM_SERIES = {
    "idhm": "ADH_IDHM",
    "idhm_education": "ADH_IDHM_E",
    "idhm_longevity": "ADH_IDHM_L",
    "idhm_income": "ADH_IDHM_R",
}

# IBGE municipal mesh (stage 1, graph). The latest edition is discovered from the directory listing.
IBGE_MESH_BASE = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/"
RAW_VOLUME = "raw_files"            # Unity Catalog volume in the bronze schema for raw files
PROJECTED_CRS = "EPSG:5880"         # SIRGAS 2000 / Brazil Polyconic: lengths and areas in metres

# Neo4j (stage 1). Credentials live in a Databricks secret scope, never in the notebooks.
NEO4J_SECRET_SCOPE = "psi"
# Every node of this project carries this extra label and a unique `psi_key` ("<Type>:<id>").
# Loads, deletes, constraints and queries are scoped to it, so other projects in the same
# database (AuraDB Free has a single database) are never touched.
NEO4J_PROJECT_LABEL = "PSI"


def volume_path(*parts: str) -> str:
    return "/".join([f"/Volumes/{CATALOG}/{SCHEMAS['bronze']}/{RAW_VOLUME}", *parts])


# Event groups (normalized event names, see normalize_text).
# CVLI - Crimes Violentos Letais Intencionais (lethal intentional violent crimes), counted in victims.
# Feminicide is a separate SINESP event, NOT included in "Homicídio doloso". Verified on the 2025-2026
# files: in 1,996 of the 2,284 municipality-months with feminicide, feminicide victims exceed the
# female victims of intentional homicide, which would be impossible if one contained the other.
CVLI_EVENTS = ["HOMICIDIO DOLOSO", "FEMINICIDIO", "ROUBO SEGUIDO DE MORTE LATROCINIO", "LESAO CORPORAL SEGUIDA DE MORTE"]
# Vehicle crimes: published by SINESP at state level only (one row per state and month).
VEHICLE_EVENT_PATTERN = r"^(ROUBO|FURTO) DE VEICULO$"
DRUG_SEIZURE_PATTERN = r"^APREENSAO DE "

# COMMAND ----------

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def http_get(url: str, retries: int = 3, backoff_seconds: int = 5, allow_insecure_fallback: bool = True):
    """GET with retries and exponential backoff.

    Some gov.br hosts serve certificate chains that are not in the cluster
    trust store. For these public, read-only downloads the request is retried
    once without TLS verification, and a warning is printed.
    """
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.SSLError:
            if not allow_insecure_fallback:
                raise
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            print(f"WARNING: TLS chain not validated for {url}; retrying without verification (public data, read-only).")
            response = requests.get(url, timeout=HTTP_TIMEOUT, verify=False)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as error:
            status = error.response.status_code if error.response is not None else None
            if status is not None and 400 <= status < 500 and status != 429:
                body = error.response.text[:500] if error.response is not None else ""
                raise RuntimeError(f"HTTP {status} for {url} (not retried, the request itself is invalid). "
                                   f"Response: {body}") from error
            last_error = error
        except requests.exceptions.RequestException as error:
            last_error = error
        if attempt < retries:
            wait = backoff_seconds * 2 ** (attempt - 1)
            print(f"Attempt {attempt}/{retries} failed for {url}: {last_error}. Retrying in {wait}s.")
            time.sleep(wait)
        else:
            print(f"Attempt {attempt}/{retries} failed for {url}: {last_error}.")
    raise RuntimeError(f"Download failed after {retries} attempts: {url}") from last_error


# ---------------------------------------------------------------------------
# Pandas -> Spark (Bronze keeps every value as string, nulls preserved)
# ---------------------------------------------------------------------------
def to_snake(label: str) -> str:
    text = unicodedata.normalize("NFKD", str(label)).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower()
    return text or "col"


def pandas_to_spark_str(pdf: pd.DataFrame) -> DataFrame:
    pdf = pdf.copy()
    columns, seen = [], {}
    for col in pdf.columns:
        name = to_snake(col)
        seen[name] = seen.get(name, 0) + 1
        columns.append(name if seen[name] == 1 else f"{name}_{seen[name]}")
    def as_text(value):
        if value is None:
            return None
        try:
            if pd.isna(value):          # NaN, NaT, pd.NA -> real null (never the text "NaN")
                return None
        except (TypeError, ValueError):  # lists/dicts are not scalars
            pass
        return str(value)

    records = [[as_text(v) for v in row] for row in pdf.itertuples(index=False, name=None)]
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(records, schema)


def try_cast(column: str, data_type: str):
    """Safe cast: returns null instead of failing (serverless runs with ANSI mode on)."""
    return F.expr(f"try_cast(`{column}` AS {data_type})")


def parse_count(column: str):
    """Parses counts published as '1234', '1234.0' or '1.234' (thousands separator)."""
    c = f"trim(`{column}`)"
    return F.expr(
        f"CASE WHEN {c} RLIKE '^[0-9]{{1,3}}([.][0-9]{{3}})+$' THEN try_cast(replace({c}, '.', '') AS BIGINT) "
        f"ELSE try_cast(try_cast({c} AS DOUBLE) AS BIGINT) END")


def add_audit(df: DataFrame, source_system: str, source_url: str) -> DataFrame:
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_system", F.lit(source_system))
              .withColumn("_source_url", F.lit(source_url)))

# COMMAND ----------

# ---------------------------------------------------------------------------
# Municipality matching
# ---------------------------------------------------------------------------
_ACCENTED = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ"
_PLAIN = "AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn"


def normalize_text(col):
    """Uppercase, no accents, single spaces. Used for display-friendly keys (e.g. event names)."""
    c = F.upper(F.translate(F.trim(col), _ACCENTED, _PLAIN))
    c = F.regexp_replace(c, r"[^A-Z0-9 ]", " ")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def name_key(col):
    """Matching key for municipality names: letters and digits only.

    Removing spaces, hyphens and apostrophes makes 'Santa Bárbara d'Oeste',
    'SANTA BARBARA D OESTE' and 'SANTA BARBARA DOESTE' collide on purpose.
    """
    return F.regexp_replace(F.upper(F.translate(F.trim(col), _ACCENTED, _PLAIN)), r"[^A-Z0-9]", "")


# Known spelling differences between sources and IBGE (state, source key -> IBGE key).
# Extend this map with the rows reported in psi_silver.qa_unmatched_municipalities.
MUNICIPALITY_ALIASES = {
    ("SP", "MOJIMIRIM"): "MOGIMIRIM",
    ("SP", "EMBU"): "EMBUDASARTES",
    ("PA", "SANTAISABELDOPARA"): "SANTAIZABELDOPARA",
    ("CE", "ITAPAGE"): "ITAPAJE",
    ("TO", "SAOVALERIODANATIVIDADE"): "SAOVALERIO",
    ("RN", "PRESIDENTEJUSCELINO"): "SERRACAIADA",
    ("RN", "AUGUSTOSEVERO"): "CAMPOGRANDE",
    ("RN", "BOASAUDE"): "JANUARIOCICCO",
    ("PB", "SANTAREM"): "JOCACLAUDINO",
    # Seen in SENATRAN (2026-09): renamed municipalities or names too different for fuzzy matching
    ("GO", "BOMJESUS"): "BOMJESUSDEGOIAS",
    ("TO", "FORTALEZADOTABOCAO"): "TABOCAO",
    ("PB", "SAODOMINGOSDEPOMBAL"): "SAODOMINGOS",
    ("RR", "SAOLUIZ"): "SAOLUIZDOANAUA",
    # Seen in SINESP (2025-2026)
    ("RN", "ACU"): "ASSU",
    # Fuzzy matches confirmed by the GenAI review (notebook 20, 2026-10) and checked by a person:
    # spelling variants, preposition swaps and names truncated at 30 characters, now matched exactly
    ("BA", "LAGEDODOTABOCAL"): "LAJEDODOTABOCAL",
    ("BA", "SANTATERESINHA"): "SANTATEREZINHA",
    ("MG", "AMPARODASERRA"): "AMPARODOSERRA",
    ("MG", "BARAOD0MONTEALTO"): "BARAODOMONTEALTO",   # digit zero in the source
    ("MG", "BARAODEMONTEALTO"): "BARAODOMONTEALTO",
    ("MG", "QUELUZITA"): "QUELUZITO",
    ("MT", "SANTOANTONIODOLEVERGER"): "SANTOANTONIODELEVERGER",
    ("MT", "VILABELADASANTISSIMATRINDA"): "VILABELADASANTISSIMATRINDADE",
    ("PA", "ELDORADODOSCARAJAS"): "ELDORADODOCARAJAS",
    ("PE", "BELEMDESAOFRANCISCO"): "BELEMDOSAOFRANCISCO",
    ("PE", "IGUARACI"): "IGUARACY",
    ("PE", "LAGOADOITAENGA"): "LAGOADEITAENGA",
    ("PI", "SAOFRANCISCODEASSISDOPIAU"): "SAOFRANCISCODEASSISDOPIAUI",
    ("PR", "BELAVISTADOCAROBA"): "BELAVISTADACAROBA",
    ("PR", "MUNHOZDEMELLO"): "MUNHOZDEMELO",
    ("PR", "PINHALDOSAOBENTO"): "PINHALDESAOBENTO",
    ("PR", "SANTACRUZDOMONTECASTELO"): "SANTACRUZDEMONTECASTELO",
    ("RJ", "PARATI"): "PARATY",
    ("RJ", "TRAJANODEMORAIS"): "TRAJANODEMORAES",
    ("RN", "ARES"): "AREZ",
    ("SC", "BALNEARIODEPICARRAS"): "BALNEARIOPICARRAS",
    ("SC", "LAGEADOGRANDE"): "LAJEADOGRANDE",
    ("SC", "PRESIDENTECASTELOBRANCO"): "PRESIDENTECASTELLOBRANCO",
    ("SC", "SAOLOURENCODOESTE"): "SAOLOURENCODOOESTE",
    ("SC", "SAOMIGUELDOESTE"): "SAOMIGUELDOOESTE",
    ("SE", "AMPARODESAOFRANCISCO"): "AMPARODOSAOFRANCISCO",
    ("SE", "GRACHOCARDOSO"): "GRACCHOCARDOSO",
    ("TO", "COUTODEMAGALHAES"): "COUTOMAGALHAES",
}

# Fuzzy matching (fallback after exact and alias): same state, small Levenshtein distance between
# name keys, and a single best candidate. Covers typos, preposition swaps (DO/DE/DOS) and names
# truncated at 30 characters. Every fuzzy match is written to qa_fuzzy_matches for review.
FUZZY_MAX_DISTANCE_SHORT = 1   # name keys with fewer than 10 characters
FUZZY_MAX_DISTANCE_LONG = 2    # name keys with 10 characters or more
NOT_INFORMED_KEYS = ["", "NAOINFORMADO", "MUNICIPIONAOINFORMADO"]


def resolve_state(df: DataFrame, state_col: str, dim_municipality: DataFrame) -> DataFrame:
    """Adds `state_code` (2-letter) from either an abbreviation or a full state name."""
    states = (dim_municipality.select("state_code", "state_name").distinct()
              .withColumn("_state_key", name_key(F.col("state_name"))))
    raw = F.upper(F.trim(F.col(state_col)))
    df = df.withColumn("_state_raw", raw).withColumn("_state_key", name_key(F.col(state_col)))
    df = df.join(F.broadcast(states.select(F.col("state_code").alias("_code_by_name"), "_state_key")), "_state_key", "left")
    valid_codes = [r.state_code for r in states.select("state_code").collect()]
    return (df.withColumn("state_code",
                          F.when(F.col("_state_raw").isin(valid_codes), F.col("_state_raw"))
                           .otherwise(F.col("_code_by_name")))
              .drop("_state_raw", "_state_key", "_code_by_name"))


def match_municipalities(df: DataFrame, state_col: str, name_col: str, dim_municipality: DataFrame, source: str):
    """Matches (state, municipality name) to the IBGE code, in three steps:

    1. exact match on the name key (after MUNICIPALITY_ALIASES);
    2. fuzzy match for what is left: same state, Levenshtein distance within the limits above,
       and a single best candidate (ties are not matched);
    3. anything else is returned as unmatched.

    Returns (matched, unmatched). `matched` has `match_type` (exact or fuzzy). `unmatched` follows
    the schema of psi_silver.qa_unmatched_municipalities. Fuzzy matches are saved to
    psi_silver.qa_fuzzy_matches for audit.
    """
    df = resolve_state(df, state_col, dim_municipality)
    df = df.withColumn("name_key", name_key(F.col(name_col)))

    aliases = spark.createDataFrame(
        [(uf, src, dst) for (uf, src), dst in MUNICIPALITY_ALIASES.items()],
        "state_code string, name_key string, alias_key string")
    df = (df.join(F.broadcast(aliases), ["state_code", "name_key"], "left")
            .withColumn("name_key", F.coalesce("alias_key", "name_key")).drop("alias_key"))

    lookup = dim_municipality.select("municipality_code", "state_code", "name_key")
    joined = df.join(F.broadcast(lookup), ["state_code", "name_key"], "left")
    exact = joined.filter(F.col("municipality_code").isNotNull()).withColumn("match_type", F.lit("exact"))
    pending = joined.filter(F.col("municipality_code").isNull()).drop("municipality_code")

    # Fuzzy step on the distinct pending names only (small), against the same state
    candidates = (pending.select("state_code", "name_key").distinct()
                  .filter(F.col("state_code").isNotNull())
                  .join(lookup.withColumnRenamed("name_key", "ibge_key"), "state_code")
                  .withColumn("distance", F.levenshtein("name_key", "ibge_key"))
                  .withColumn("max_distance", F.when(F.length("name_key") >= 10, FUZZY_MAX_DISTANCE_LONG)
                                               .otherwise(FUZZY_MAX_DISTANCE_SHORT))
                  .filter(F.col("distance") <= F.col("max_distance")))
    ranked = (candidates
              .withColumn("_best", F.min("distance").over(Window.partitionBy("state_code", "name_key")))
              .filter(F.col("distance") == F.col("_best"))
              .withColumn("_ties", F.count(F.lit(1)).over(Window.partitionBy("state_code", "name_key"))))
    fuzzy_map = (ranked.filter(F.col("_ties") == 1)
                 .select("state_code", "name_key", "municipality_code", "ibge_key", "distance"))

    fuzzy = (pending.join(F.broadcast(fuzzy_map), ["state_code", "name_key"], "inner")
             .drop("ibge_key", "distance").withColumn("match_type", F.lit("fuzzy")))
    still_unmatched = pending.join(F.broadcast(fuzzy_map.select("state_code", "name_key")),
                                   ["state_code", "name_key"], "left_anti")

    audit = (pending.join(F.broadcast(fuzzy_map), ["state_code", "name_key"], "inner")
             .join(dim_municipality.select("municipality_code", "municipality_name"), "municipality_code")
             .groupBy("state_code", F.col(name_col).alias("source_name"), "municipality_code",
                      F.col("municipality_name").alias("ibge_name"), "distance")
             .agg(F.count(F.lit(1)).alias("rows"))
             .withColumn("source", F.lit(source))
             .withColumn("checked_at", F.current_timestamp()))
    write_source_qa(audit, "qa_fuzzy_matches", source)

    matched = exact.unionByName(fuzzy)
    unmatched = (still_unmatched
                 .groupBy(F.col(state_col).alias("source_state"), F.col(name_col).alias("source_name"),
                          "state_code", "name_key")
                 .agg(F.count(F.lit(1)).alias("rows"))
                 .withColumn("source", F.lit(source))
                 .withColumn("checked_at", F.current_timestamp()))
    return matched, unmatched


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------
def assert_unique(df: DataFrame, keys: list, label: str) -> None:
    duplicates = df.groupBy(*keys).count().filter("count > 1")
    total = duplicates.count()
    if total:
        duplicates.show(20, truncate=False)
        raise AssertionError(f"{label}: {total} duplicated key(s) on {keys}")
    print(f"OK  {label}: unique on {keys}")


def assert_not_empty(df: DataFrame, label: str) -> int:
    total = df.count()
    if total == 0:
        raise AssertionError(f"{label}: no rows")
    print(f"OK  {label}: {total:,} rows")
    return total


def nan_to_null(df: DataFrame) -> DataFrame:
    """pandas uses NaN for missing numbers; Spark keeps it as a value. Convert to real nulls."""
    float_types = {"double", "float"}
    return df.select([F.when(~F.isnan(c), F.col(c)).alias(c) if t in float_types else F.col(c)
                      for c, t in df.dtypes])


def write_table(df: DataFrame, layer: str, name: str, partition_by=None) -> None:
    target = tbl(layer, name)
    writer = df.write.format(TABLE_FORMAT).mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target)
    print(f"Saved {target}: {spark.table(target).count():,} rows")


def write_source_qa(rows: DataFrame, table: str, source: str) -> None:
    """Replaces the QA rows of one source in a psi_silver QA table (other sources are kept)."""
    target = tbl("silver", table)
    if spark.catalog.tableExists(target):
        kept = spark.table(target).filter(F.col("source") != source)
        if set(kept.columns) == set(rows.columns):
            combined = kept.unionByName(rows)
            # Small table: materialize before overwriting the table it was read from
            # (works on serverless compute, where cache/checkpoint are not available).
            rows = spark.createDataFrame(combined.collect(), combined.schema)
    write_table(rows, "silver", table)


def write_unmatched(unmatched: DataFrame, source: str) -> None:
    write_source_qa(unmatched, "qa_unmatched_municipalities", source)
    print(f"Unmatched names for {source}: {unmatched.count():,}")
    fuzzy = spark.table(tbl("silver", "qa_fuzzy_matches")).filter(F.col("source") == source)
    print(f"Fuzzy matches for {source}: {fuzzy.count():,} (review in {tbl('silver', 'qa_fuzzy_matches')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental processing
# MAGIC
# MAGIC Every notebook that writes a dataset records it in **`psi_ops.change_log`** (dataset, key such as a year or a month, content hash, rows, time). Every notebook that reads datasets records in **`psi_ops.consumer_log`** up to which change it has processed. With the two logs:
# MAGIC
# MAGIC | Layer | Behaviour on a new run |
# MAGIC |---|---|
# MAGIC | Bronze | Downloads the source and compares the **SHA-256 of the file** with the last load. Unchanged files are not written again. SINESP is stored by year (`_source_year`) and SENATRAN by month (`_reference_month`): only the changed year or month is replaced, and older months are kept as history. |
# MAGIC | Silver and Gold facts | SINESP occurrences are reprocessed **only for the years that changed** (`replace_partitions`), in Silver and in the Gold facts. |
# MAGIC | Small derived tables | Dimensions, indicators, analytics, spatial metrics and the Neo4j graph are rebuilt only when one of their inputs changed; otherwise the notebook ends at the start with "nothing to do" (`run_or_exit`). They are small (one row per municipality or state), so rebuilding them is cheaper and safer than merging. |
# MAGIC
# MAGIC **Full refresh.** `FULL_REFRESH = True` (or the job parameter `full_refresh=true`) ignores the logs and rebuilds everything, e.g. after changing a business rule such as `CVLI_EVENTS` or `WINDOW_MONTHS`, which does not change any input data.

# COMMAND ----------

import hashlib
from datetime import datetime, timezone

FULL_REFRESH = False
try:  # job parameter or notebook widget "full_refresh", when defined
    FULL_REFRESH = dbutils.widgets.get("full_refresh").strip().lower() == "true"
except Exception:
    pass

CHANGE_LOG = "change_log"
CONSUMER_LOG = "consumer_log"
_CHANGE_LOG_SCHEMA = "dataset string, key string, content_sha256 string, rows bigint, changed_at timestamp"
_CONSUMER_LOG_SCHEMA = "consumer string, dataset string, consumed_until timestamp, consumed_at timestamp"
_HIGH_WATER = {}   # consumer -> {dataset: latest changed_at read at the start of the run}


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _ops_table(name: str):
    target = tbl("ops", name)
    return spark.table(target) if spark.catalog.tableExists(target) else None


def _append_ops(rows: list, schema: str, name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMAS['ops']}" if CATALOG
              else f"CREATE SCHEMA IF NOT EXISTS {SCHEMAS['ops']}")
    (spark.createDataFrame(rows, schema).write.format(TABLE_FORMAT).mode("append")
     .saveAsTable(tbl("ops", name)))


def is_unchanged(dataset: str, key, content_sha256: str) -> bool:
    """True when the last load of this dataset and key had the same content hash."""
    log = _ops_table(CHANGE_LOG)
    if FULL_REFRESH or log is None:
        return False
    last = (log.filter((F.col("dataset") == dataset) & (F.col("key") == str(key)))
            .orderBy(F.desc("changed_at")).select("content_sha256").first())
    return last is not None and last.content_sha256 == content_sha256


def log_change(dataset: str, keys, content_sha256: str = None, rows: int = None) -> None:
    """Records that `keys` of `dataset` were (re)written now."""
    now = datetime.now(timezone.utc)
    _append_ops([(dataset, str(k), content_sha256, rows, now) for k in keys], _CHANGE_LOG_SCHEMA, CHANGE_LOG)


def changes_since_last_run(consumer: str, datasets: list) -> dict:
    """Keys changed in each input dataset since `consumer` last finished ({dataset: [keys]}).
    With FULL_REFRESH, every key ever logged. The high-water mark read here is the one that
    finish_step() records, so a change that arrives during the run is processed next time."""
    log = _ops_table(CHANGE_LOG)
    consumed = None if FULL_REFRESH else _ops_table(CONSUMER_LOG)
    pending, marks = {}, {}
    for dataset in datasets:
        changes = log.filter(F.col("dataset") == dataset) if log is not None else None
        if changes is None:
            pending[dataset], marks[dataset] = [], None
            continue
        marks[dataset] = changes.agg(F.max("changed_at")).first()[0]
        if consumed is not None:
            last = (consumed.filter((F.col("consumer") == consumer) & (F.col("dataset") == dataset))
                    .agg(F.max("consumed_until")).first()[0])
            if last is not None:
                changes = changes.filter(F.col("changed_at") > F.lit(last))
        pending[dataset] = sorted({r.key for r in changes.select("key").distinct().collect()})
    _HIGH_WATER[consumer] = marks
    return pending


def finish_step(consumer: str) -> None:
    """Records that `consumer` processed its inputs up to the high-water mark of this run."""
    now = datetime.now(timezone.utc)
    rows = [(consumer, dataset, mark, now) for dataset, mark in _HIGH_WATER.pop(consumer, {}).items()
            if mark is not None]
    if rows:
        _append_ops(rows, _CONSUMER_LOG_SCHEMA, CONSUMER_LOG)


def run_or_exit(consumer: str, datasets: list, outputs: list = (), output_missing: bool = False) -> dict:
    """Start of a notebook that derives tables from `datasets`. Returns the changed keys per input.
    When nothing changed and every output exists, the notebook ends here (dbutils.notebook.exit).
    `outputs` are (layer, table) pairs; `output_missing` covers outputs that are not tables."""
    pending = changes_since_last_run(consumer, datasets)
    missing = output_missing or any(not spark.catalog.tableExists(tbl(*o)) for o in outputs)
    changed = {d: keys for d, keys in pending.items() if keys}
    if FULL_REFRESH or missing or changed:
        reason = "full refresh" if FULL_REFRESH else "output missing" if missing else f"changes: {changed}"
        print(f"{consumer}: processing ({reason})")
        return pending
    message = f"{consumer}: no changes in {', '.join(datasets)} since the last run; nothing to do."
    print(message)
    dbutils.notebook.exit(message)


def replace_partitions(df: DataFrame, layer: str, name: str, column: str, values) -> None:
    """Writes `df` replacing only the partitions `column IN values` (e.g. the SINESP years that
    changed); the other partitions are kept. Creates the table on the first run, rewrites it with
    FULL_REFRESH, and merges the new columns when the schema changes."""
    target = tbl(layer, name)
    values = sorted(set(values))
    if FULL_REFRESH or not spark.catalog.tableExists(target):
        write_table(df, layer, name, partition_by=[column])
        return
    existing = spark.table(target)
    if column not in existing.columns:
        print(f"WARNING {target}: table without the partition column `{column}` (created by an earlier version); "
              f"rewritten with the data of this run ({column} in {values}). Run with FULL_REFRESH if other values are missing.")
        write_table(df, layer, name, partition_by=[column])
        return
    if set(existing.columns) != set(df.columns):
        print(f"{target}: schema changed; rewriting it with the other partitions kept")
        df = existing.filter(~F.col(column).isin(values)).unionByName(df, allowMissingColumns=True)
        if TABLE_FORMAT != "delta":   # Parquet (local tests) cannot overwrite a table it is reading
            df = spark.createDataFrame(df.collect(), df.schema)
        write_table(df, layer, name, partition_by=[column])
        return
    if TABLE_FORMAT == "delta":
        literals = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
        (df.write.format("delta").mode("overwrite").option("replaceWhere", f"{column} IN ({literals})")
         .saveAsTable(target))
    else:                             # Parquet (local tests): dynamic partition overwrite
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.select(*existing.columns).write.insertInto(target, overwrite=True)
    print(f"Saved {target}: partitions {column} in {values} replaced; {spark.table(target).count():,} rows in total")


def write_if_changed(name: str, content: bytes, build) -> bool:
    """Bronze table loaded from a single source response: written (and logged) only when the
    content hash differs from the last load. `build` returns the DataFrame. Returns True if written."""
    digest = sha256(content)
    if is_unchanged(name, "all", digest) and spark.catalog.tableExists(tbl("bronze", name)):
        print(f"{name}: source unchanged since the last load, skipped")
        return False
    df = build()
    rows = assert_not_empty(df, name)
    write_table(df, "bronze", name)
    log_change(name, ["all"], digest, rows)
    return True
