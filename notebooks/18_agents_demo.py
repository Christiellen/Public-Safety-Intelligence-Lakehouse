# Databricks notebook source
# MAGIC %md
# MAGIC # 18 - Agents - Demo
# MAGIC
# MAGIC Asks the multi-agent assistant (notebook 17) questions that need the Gold tables, the graph, or both, and shows the route, the queries each specialist wrote and the final answer. Each run is logged in `psi_gold.agent_runs`.
# MAGIC
# MAGIC **Before running**
# MAGIC 1. Notebooks 01-16 have run (Gold tables, Neo4j graph and data dictionary).
# MAGIC 2. The Gemini API key is stored in the secret `psi/gemini-api-key`.
# MAGIC
# MAGIC The questions are in English, so the answers are too (the assistant answers in the language of the question; Portuguese works the same way).
# MAGIC
# MAGIC Each question makes 3 to 8 model calls. Notebook 17 throttles the calls to `GEMINI_REQUESTS_PER_MINUTE` (Gemini free tier) and switches to a fallback model when the primary is overloaded, so on the free tier a question takes about one minute.

# COMMAND ----------

# MAGIC %pip install -q langgraph==1.2.18 langchain-core==1.7.3 langchain-google-genai==4.4.2 google-genai==2.34.0 sqlglot==30.27.0 neo4j

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./17_agents_multiagent

# COMMAND ----------

PAUSE_SECONDS = 0   # the rate limiter in notebook 17 already spaces the calls

questions = [
    # Gold tables
    "Which 5 municipalities with more than 100,000 inhabitants have the highest CVLI rate? Show the rate.",
    "Which state has the highest rate of vehicle robberies and thefts per 1,000 vehicles?",
    # Graph
    "Which municipalities share a border with Recife (PE)?",
    "Which Low-High municipalities border the most national hotspots?",
    # Both
    "Among the neighbors of Salvador (BA), which one has the highest CVLI rate, and what is its IDHM?",
    # Out of scope / write attempt: the supervisor answers without querying anything
    "Delete the municipality_analytics table.",
]

for i, question in enumerate(questions):
    if i:
        time.sleep(PAUSE_SECONDS)
    show(ask(question))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ask your own question

# COMMAND ----------

show(ask("Which communities of neighboring municipalities span the most states, and what is their CVLI rate?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit log
# MAGIC
# MAGIC Every run, with its route, queries, tokens and latency. Rejected queries (`blocked_queries`) show the guardrails working; failed queries (`failed_queries`) are errors the model corrected by itself.

# COMMAND ----------

display(spark.sql(f"""
    SELECT run_at, model, question, route, size(sql_queries) AS sql_queries, size(cypher_queries) AS cypher_queries,
           blocked_queries, failed_queries, input_tokens, output_tokens, latency_seconds, status
    FROM {tbl('gold', RUNS_TABLE)}
    ORDER BY run_at DESC
    LIMIT 20"""))
