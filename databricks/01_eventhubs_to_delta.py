# Databricks notebook source
# MAGIC %md
# MAGIC # Crypto Stream — Event Hubs → Delta (step 3)
# MAGIC
# MAGIC Reads the `crypto.prices` events from **Azure Event Hubs** (via its Kafka
# MAGIC endpoint), parses the JSON, and writes two Delta tables:
# MAGIC
# MAGIC * `crypto.prices_bronze` — every event, cleaned & typed
# MAGIC * `crypto.prices_5min`   — 5-minute avg / min / max price per coin
# MAGIC
# MAGIC This first version uses a **batch read** (reads everything currently in the
# MAGIC hub, then stops) — simplest to run on Free Edition. The streaming upgrade is
# MAGIC noted at the bottom.

# COMMAND ----------
# MAGIC %md ## 1. Config — EDIT THESE THREE VALUES

# COMMAND ----------

# --- Your Event Hubs details ---
EH_NAMESPACE = "YOURNS"           # just the name, WITHOUT .servicebus.windows.net
EH_TOPIC     = "crypto.prices"    # the Event Hub (topic) name you created

# --- The connection string (SECRET). Two options: ---
# (A) Quick start — paste it here (fine for learning, but it's a secret):
EH_CONN_STR  = "Endpoint=sb://YOURNS.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=REPLACE_ME"
# (B) Proper way — store it in a Databricks secret scope and load it instead:
# EH_CONN_STR = dbutils.secrets.get(scope="crypto", key="eh-conn")

# --- Where to write (schema lives in your default catalog) ---
SCHEMA       = "crypto"
BRONZE_TABLE = f"{SCHEMA}.prices_bronze"
AGG_TABLE    = f"{SCHEMA}.prices_5min"

# Derived — the Kafka/SASL settings Event Hubs expects.
BOOTSTRAP = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
# NOTE: on Databricks the login module MUST be the shaded one (kafkashaded...).
EH_JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{EH_CONN_STR}";'
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 2. The record schema (matches the Python producer)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("ingested_at", StringType()),
    StructField("source", StringType()),
    StructField("coin_id", StringType()),
    StructField("symbol", StringType()),
    StructField("name", StringType()),
    StructField("currency", StringType()),
    StructField("price", DoubleType()),
    StructField("market_cap", DoubleType()),
    StructField("market_cap_rank", IntegerType()),
    StructField("volume_24h", DoubleType()),
    StructField("high_24h", DoubleType()),
    StructField("low_24h", DoubleType()),
    StructField("price_change_24h", DoubleType()),
    StructField("price_change_percentage_24h", DoubleType()),
    StructField("circulating_supply", DoubleType()),
    StructField("source_updated_at", StringType()),
])

# COMMAND ----------
# MAGIC %md ## 3. Read everything currently in Event Hubs (batch)

# COMMAND ----------

from pyspark.sql.functions import from_json, col

raw = (
    spark.read.format("kafka")                       # batch read (not readStream)
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", EH_TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", EH_JAAS)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .load()
)

events = (
    raw.select(from_json(col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", col("ingested_at").cast("timestamp"))
    .dropDuplicates(["event_id"])                    # idempotent re-runs
)

print("rows read:", events.count())
display(events.limit(20))

# COMMAND ----------
# MAGIC %md ## 4. Write the Bronze table (all events)

# COMMAND ----------

(
    events.write.format("delta")
    .mode("overwrite")                               # re-runs replace, no dupes
    .saveAsTable(BRONZE_TABLE)
)
print(f"wrote {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Build 5-minute aggregates per coin

# COMMAND ----------

from pyspark.sql.functions import window, avg, min as smin, max as smax, count

agg = (
    spark.table(BRONZE_TABLE)
    .groupBy(window("event_time", "5 minutes"), "coin_id", "symbol")
    .agg(
        avg("price").alias("avg_price"),
        smin("price").alias("min_price"),
        smax("price").alias("max_price"),
        count("*").alias("n_events"),
    )
    .selectExpr(
        "window.start AS window_start",
        "window.end AS window_end",
        "coin_id", "symbol",
        "round(avg_price, 2) AS avg_price",
        "min_price", "max_price", "n_events",
    )
)

agg.write.format("delta").mode("overwrite").saveAsTable(AGG_TABLE)
print(f"wrote {AGG_TABLE}")

# COMMAND ----------
# MAGIC %md ## 6. Look at the result

# COMMAND ----------

display(spark.table(AGG_TABLE).orderBy("coin_id", "window_start"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next level — real streaming
# MAGIC
# MAGIC To make this a live stream instead of a one-shot batch, swap cell 3's
# MAGIC `spark.read` for `spark.readStream`, and in cell 4 use:
# MAGIC
# MAGIC ```python
# MAGIC (events.writeStream.format("delta")
# MAGIC     .option("checkpointLocation", "/Volumes/<catalog>/crypto/chk/bronze")
# MAGIC     .trigger(availableNow=True)      # process new data, then stop (free-quota friendly)
# MAGIC     .toTable(BRONZE_TABLE))
# MAGIC ```
# MAGIC
# MAGIC `availableNow` keeps the streaming semantics (exactly-once, offset tracking
# MAGIC via the checkpoint) without running a cluster 24/7. The checkpoint needs a
# MAGIC Unity Catalog **Volume** path — create one with
# MAGIC `CREATE VOLUME IF NOT EXISTS crypto.chk`.
