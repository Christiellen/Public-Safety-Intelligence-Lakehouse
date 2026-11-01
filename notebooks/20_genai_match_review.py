# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - GenAI - Review of fuzzy municipality matches (entity resolution)
# MAGIC
# MAGIC SINESP and SENATRAN name municipalities in free text. The pipeline matches those names to the official IBGE code by exact name, then by the curated aliases, then by a conservative **fuzzy match** (same state, Levenshtein distance up to 2, a single best candidate). Every fuzzy match is kept in `psi_silver.qa_fuzzy_matches`, because a small spelling difference can still be another municipality (*Itapeva* and *Itapevi* are both in São Paulo).
# MAGIC
# MAGIC This notebook uses Gemini as a **reviewer** of those matches, the way a data steward would:
# MAGIC
# MAGIC 1. for each fuzzy match it sends the source name, the matched IBGE municipality and the closest other IBGE names of the same state;
# MAGIC 2. the model answers, in JSON, whether it is the same municipality, another candidate, none of them (for example a renamed municipality) or uncertain, with a confidence and a short reason;
# MAGIC 3. **the answer is checked in code**: a suggested IBGE code must be one of the candidates sent, and a suggested official name must exist in the IBGE list of that state, so the model cannot invent a municipality;
# MAGIC 4. each decision becomes an action - `confirm` (keep the match and turn it into an alias), `fix` (the match is wrong; alias to the suggested municipality) or `human review` - and the reviews are appended to `psi_silver.qa_match_review`;
# MAGIC 5. the confirmed and fixed matches are printed as entries for `MUNICIPALITY_ALIASES` (notebook 00). **Nothing changes in the pipeline until a person adds them**: the model proposes, a person decides.
# MAGIC
# MAGIC The review is incremental: a match already reviewed with the same prompt version is not sent again. A small labeled set (`GOLDEN_SET`) with real look-alike names measures the accuracy of the reviewer before its suggestions are used.
# MAGIC
# MAGIC **Before running:** notebooks 05-07 have run (fuzzy matches and IBGE dimension) and the Gemini key is in the secret `psi/gemini-api-key`. Runs on demand (not in the scheduled job), like the AI assistant.

# COMMAND ----------

# MAGIC %pip install -q langgraph==1.2.18 langchain-core==1.7.3 langchain-google-genai==4.4.2 google-genai==2.34.0 sqlglot==30.27.0 neo4j

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./17_agents_multiagent

# COMMAND ----------

import hashlib
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, SystemMessage

REVIEW_TABLE = "qa_match_review"
PROMPT_VERSION = "v1"          # change it when the prompt changes: every match is reviewed again
CANDIDATES_PER_MATCH = 5       # IBGE names of the same state sent with each match
BATCH_SIZE = 20                # matches per model call
RUN_EVALUATION = True          # measure the reviewer on GOLDEN_SET (one extra model call)

DECISIONS = {"same", "other", "none", "uncertain"}
CONFIDENCES = {"high", "medium", "low"}

# Labeled cases with real names: (state, name as written in a source, IBGE name proposed, expected).
# "same": spelling variant, truncation or official rename of the same municipality;
# "different": another municipality with a similar name in the same state.
GOLDEN_SET = [
    ("SP", "MOJI MIRIM", "Mogi Mirim", "same"),
    ("SP", "EMBU", "Embu das Artes", "same"),
    ("CE", "ITAPAGE", "Itapajé", "same"),
    ("PA", "SANTA ISABEL DO PARA", "Santa Izabel do Pará", "same"),
    ("RN", "PRESIDENTE JUSCELINO", "Serra Caiada", "same"),
    ("SP", "SAO LUIS DO PARAITINGA", "São Luiz do Paraitinga", "same"),
    ("PE", "BELEM DE SAO FRANCISCO", "Belém do São Francisco", "same"),
    ("SP", "ITAPEVA", "Itapevi", "different"),
    ("SP", "ITAPUI", "Itapura", "different"),
    ("SP", "BAURU", "Barueri", "different"),
    ("SP", "MAIRIPORA", "Mairinque", "different"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Matches to review

# COMMAND ----------

dim = spark.table(tbl("silver", "dim_municipality")).select("municipality_code", "municipality_name", "state_code", "name_key")


def review_key(source: str, state: str, source_name: str, code: str) -> str:
    return hashlib.sha256(f"{source}|{state}|{source_name}|{code}|{PROMPT_VERSION}".encode()).hexdigest()


def with_candidates(pairs):
    """pairs: DataFrame with state_code, source_name, matched_code. Adds the closest IBGE names of the same
    state (Levenshtein on the name key), the matched one included, as a list of (code, name)."""
    keyed = pairs.withColumn("_key", name_key(F.col("source_name")))
    ranked = (keyed.join(dim.withColumnRenamed("name_key", "_ibge_key"), "state_code")
              .withColumn("_distance", F.levenshtein("_key", "_ibge_key"))
              .withColumn("_rank", F.row_number().over(
                  Window.partitionBy("state_code", "source_name", "matched_code")
                  .orderBy(F.col("municipality_code") != F.col("matched_code"), "_distance", "municipality_code")))
              .filter(F.col("_rank") <= CANDIDATES_PER_MATCH))
    return (ranked.groupBy("state_code", "source_name", "matched_code")
            .agg(F.sort_array(F.collect_list(F.struct("_rank", "municipality_code", "municipality_name")))
                 .alias("_candidates"))
            .withColumn("candidates", F.transform("_candidates", lambda c: F.struct(
                c["municipality_code"].alias("code"), c["municipality_name"].alias("name"))))
            .drop("_candidates"))


fuzzy = (spark.table(tbl("silver", "qa_fuzzy_matches"))
         .groupBy("source", "state_code", "source_name",
                  F.col("municipality_code").alias("matched_code"), F.col("ibge_name").alias("matched_name"))
         .agg(F.sum("rows").alias("rows")))
pending = fuzzy.join(with_candidates(fuzzy.select("state_code", "source_name", "matched_code").distinct()),
                     ["state_code", "source_name", "matched_code"])

review_target = tbl("silver", REVIEW_TABLE)
reviewed = set()
if spark.catalog.tableExists(review_target) and not FULL_REFRESH:
    reviewed = {r.review_key for r in spark.table(review_target).select("review_key").collect()}

items = []
for row in pending.orderBy("source", "state_code", "source_name").collect():
    key = review_key(row.source, row.state_code, row.source_name, row.matched_code)
    if key not in reviewed:
        items.append({"review_key": key, "source": row.source, "state_code": row.state_code,
                      "source_name": row.source_name, "matched_code": row.matched_code,
                      "matched_name": row.matched_name, "rows": int(row.rows),
                      "candidates": [{"code": c.code, "name": c.name} for c in row.candidates]})
print(f"Fuzzy matches: {fuzzy.count():,} | already reviewed ({PROMPT_VERSION}): {len(reviewed):,} | to review: {len(items):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reviewer (Gemini) and validation of its answers

# COMMAND ----------

REVIEW_PROMPT = """You review automatic matches between municipality names written in Brazilian public datasets
(police records, vehicle registry) and the official IBGE list of municipalities.

For each item decide whether source_name refers to the matched municipality. Consider spelling variants, missing
accents, prepositions (de/do/da/dos), abbreviations, names truncated at 30 characters and official renames.
Different municipalities can have very similar names in the same state (Itapeva and Itapevi are both in SP):
a small spelling difference is NOT enough to say they are the same.

Answer ONLY with JSON: {"reviews": [{"id": <item id>, "decision": "...", "code": "...", "official_name": "...",
"confidence": "...", "reason": "..."}]}, one review per item:
- decision "same": source_name is the matched municipality;
- decision "other": source_name is another municipality of the candidates list; put its code in "code";
- decision "none": none of the candidates; if you know the current official name (for example after a rename),
  put it in "official_name";
- decision "uncertain": you cannot tell.
- confidence: "high" only when you are sure, otherwise "medium" or "low";
- reason: one short sentence in English.
Never invent IBGE codes: use only codes from the candidates of the same item."""


def ask_reviewer(llm, batch: list) -> tuple:
    """Sends one batch; returns ({id: raw review}, usage)."""
    payload = [{"id": i, "state": it["state_code"], "source_name": it["source_name"],
                "matched": {"code": it["matched_code"], "name": it["matched_name"]},
                "candidates": it["candidates"]} for i, it in enumerate(batch)]
    message = llm.invoke([SystemMessage(content=REVIEW_PROMPT),
                          HumanMessage(content=json.dumps(payload, ensure_ascii=False))])
    text = text_of(message)
    match = re.search(r"\{.*\}", text, re.DOTALL)
    try:
        reviews = json.loads(match.group(0))["reviews"]
        by_id = {int(r["id"]): r for r in reviews if isinstance(r, dict) and "id" in r}
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        by_id = {}
    return by_id, usage_of(message)


def validate(item: dict, raw: dict | None, names_by_key: dict) -> dict:
    """Turns a raw review into a checked one. Anything the code cannot verify becomes 'uncertain'."""
    if raw is None:
        return {"decision": "uncertain", "confidence": "low", "suggested_code": None,
                "reason": "No valid answer from the model for this item.", "validation": "missing answer"}
    decision = str(raw.get("decision", "")).strip().lower()
    confidence = str(raw.get("confidence", "")).strip().lower()
    reason = str(raw.get("reason", "")).strip()[:300]
    confidence = confidence if confidence in CONFIDENCES else "low"
    validation, suggested = "ok", None
    candidate_codes = {c["code"] for c in item["candidates"]}
    if decision == "same":
        suggested = item["matched_code"]
    elif decision == "other":
        code = str(raw.get("code", "")).strip()
        if code in candidate_codes and code != item["matched_code"]:
            suggested = code
        else:
            decision, validation = "uncertain", f"code {code!r} is not another candidate"
    elif decision == "none":
        official = str(raw.get("official_name") or "").strip()
        if official:
            found = names_by_key.get((item["state_code"], plain_key(official)))
            if found:
                suggested = found
            else:
                validation = f"official name {official!r} not found in IBGE ({item['state_code']})"
    else:
        decision = "uncertain"
    return {"decision": decision, "confidence": confidence, "suggested_code": suggested,
            "reason": reason, "validation": validation}


def plain_key(text: str) -> str:
    """Same rule as name_key() (notebook 00), in Python."""
    import unicodedata
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().upper()
    return re.sub(r"[^A-Z0-9]", "", plain)


def action_of(review: dict, item: dict) -> str:
    if review["confidence"] != "high" or review["suggested_code"] is None:
        return "human review"
    return "confirm" if review["suggested_code"] == item["matched_code"] else "fix"


def review_all(llm, batch_items: list, model_name: str) -> list:
    names_by_key = {(r.state_code, r.name_key): r.municipality_code for r in dim.collect()}
    out = []
    for start in range(0, len(batch_items), BATCH_SIZE):
        batch = batch_items[start:start + BATCH_SIZE]
        by_id, usage = ask_reviewer(llm, batch)
        for i, item in enumerate(batch):
            review = validate(item, by_id.get(i), names_by_key)
            out.append({**item, **review, "action": action_of(review, item),
                        "model": usage.get("model") or model_name})
        print(f"Reviewed {min(start + BATCH_SIZE, len(batch_items))}/{len(batch_items)}")
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Evaluation on the labeled set

# COMMAND ----------

llms, chain = build_llms()
reviewer = ModelChain(chain, llms, retired=set())
print(f"Reviewer: {chain[0]} (fallbacks: {chain[1:]})")


def evaluate(llm) -> pd.DataFrame:
    """Accuracy of the reviewer on GOLDEN_SET: 'same' must be answered for the same municipality,
    and anything else (other, none, uncertain) for different ones."""
    codes = {(r.state_code, r.name_key): (r.municipality_code, r.municipality_name) for r in dim.collect()}
    cases = []
    for state, source_name, proposed, expected in GOLDEN_SET:
        found = codes.get((state, plain_key(proposed)))
        if found:
            cases.append(({"state_code": state, "source_name": source_name, "matched_code": found[0],
                           "matched_name": found[1], "candidates": [{"code": found[0], "name": found[1]}]}, expected))
    by_id, _ = ask_reviewer(llm, [c for c, _ in cases])
    rows = []
    for i, (case, expected) in enumerate(cases):
        decision = str((by_id.get(i) or {}).get("decision", "uncertain")).lower()
        rows.append({"state": case["state_code"], "source_name": case["source_name"], "proposed": case["matched_name"],
                     "expected": expected, "decision": decision,
                     "correct": (decision == "same") == (expected == "same")})
    return pd.DataFrame(rows)


if RUN_EVALUATION:
    evaluation = evaluate(reviewer)
    accuracy = evaluation.correct.mean() if len(evaluation) else float("nan")
    print(f"Golden set: {evaluation.correct.sum()}/{len(evaluation)} correct ({accuracy:.0%})")
    display(evaluation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Review the pending matches

# COMMAND ----------

REVIEW_SCHEMA = """review_key string, source string, state_code string, source_name string, matched_code string,
matched_name string, rows bigint, decision string, confidence string, suggested_code string, reason string,
validation string, action string, model string, prompt_version string, reviewed_at timestamp"""

if items:
    results = review_all(reviewer, items, chain[0])
    now = datetime.now(timezone.utc)
    records = [(r["review_key"], r["source"], r["state_code"], r["source_name"], r["matched_code"], r["matched_name"],
                r["rows"], r["decision"], r["confidence"], r["suggested_code"], r["reason"], r["validation"], r["action"],
                r["model"], PROMPT_VERSION, now) for r in results]
    new_reviews = spark.createDataFrame(records, REVIEW_SCHEMA)
    mode = "overwrite" if FULL_REFRESH or not spark.catalog.tableExists(review_target) else "append"
    writer = new_reviews.write.format(TABLE_FORMAT).mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(review_target)
    print(f"Saved {len(records):,} reviews to {review_target} ({mode})")
else:
    print("Nothing to review: every fuzzy match was already reviewed with this prompt version.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Result and proposed aliases (for a person to apply)

# COMMAND ----------

if spark.catalog.tableExists(review_target):
    latest = (spark.table(review_target).filter(F.col("prompt_version") == PROMPT_VERSION)
              .join(dim.select(F.col("municipality_code").alias("suggested_code"),
                               F.col("municipality_name").alias("suggested_name"),
                               F.col("name_key").alias("suggested_key")), "suggested_code", "left"))
    display(latest.groupBy("action", "decision", "confidence").count().orderBy("action", "decision"))
    display(latest.filter(F.col("action") != "confirm")
            .select("source", "state_code", "source_name", "matched_name", "decision", "confidence",
                    "suggested_name", "reason", "validation").orderBy("action", "state_code"))

    proposals = (latest.filter(F.col("action").isin("confirm", "fix"))
                 .select("action", "state_code", "source_name", "suggested_key", "suggested_name")
                 .distinct().orderBy("action", "state_code", "source_name").collect())
    print(f"Proposed MUNICIPALITY_ALIASES entries ({len(proposals)}), to review and paste in notebook 00:")
    for p in proposals:
        print(f'    ("{p.state_code}", "{plain_key(p.source_name)}"): "{p.suggested_key}",'
              f'   # {p.action}: {p.source_name} -> {p.suggested_name}')
