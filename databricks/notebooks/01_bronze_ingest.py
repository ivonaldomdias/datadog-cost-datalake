# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze — Ingestão Raw do Datadog via Auto Loader
# MAGIC
# MAGIC Este notebook lê os arquivos Parquet do S3 de forma incremental usando
# MAGIC o **Auto Loader** do Databricks e persiste na camada Bronze do Delta Lake.
# MAGIC
# MAGIC **Fonte:** `s3://{S3_BUCKET}/datadog-costs/raw/`
# MAGIC **Destino:** `datadog_costs.bronze_raw`
# MAGIC
# MAGIC **Executar:** Diariamente após o pipeline de extração Python

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# Parâmetros — configurar no Job ou via widgets
dbutils.widgets.text("s3_bucket", "", "S3 Bucket")
dbutils.widgets.text("checkpoint_path", "/mnt/datadog-costs/checkpoints/bronze", "Checkpoint Path")
dbutils.widgets.dropdown("product_filter", "all",
                         ["all", "infrastructure", "apm", "logs", "estimated_cost"],
                         "Product Filter")

S3_BUCKET       = dbutils.widgets.get("s3_bucket")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
PRODUCT_FILTER  = dbutils.widgets.get("product_filter")

S3_SOURCE_PATH = f"s3://{S3_BUCKET}/datadog-costs/raw/"
BRONZE_TABLE   = "datadog_costs.bronze_raw"
DATABASE       = "datadog_costs"

print(f"Source: {S3_SOURCE_PATH}")
print(f"Target: {BRONZE_TABLE}")
print(f"Product filter: {PRODUCT_FILTER}")

# COMMAND ----------

# MAGIC %md ## Criar Database e Tabela Bronze

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    product         STRING  COMMENT 'Produto Datadog (infrastructure, apm, logs, estimated_cost)',
    date            STRING  COMMENT 'Data de referência YYYY-MM-DD',
    org_name        STRING  COMMENT 'Nome da organização Datadog',
    metric_name     STRING  COMMENT 'Nome da métrica ou produto de custo',
    value           DOUBLE  COMMENT 'Valor da métrica ou custo em USD',
    unit            STRING  COMMENT 'Unidade da métrica (hosts, bytes, usd)',
    extracted_at    TIMESTAMP COMMENT 'Timestamp de extração UTC',
    _source_file    STRING  COMMENT 'Arquivo S3 de origem',
    _ingested_at    TIMESTAMP COMMENT 'Timestamp de ingestão no Delta Lake'
)
USING DELTA
PARTITIONED BY (product, date)
COMMENT 'Camada Bronze: dados brutos do Datadog sem transformação'
LOCATION 's3://{S3_BUCKET}/datadog-costs/delta/bronze/'
""")

print(f"✅ Tabela {BRONZE_TABLE} pronta")

# COMMAND ----------

# MAGIC %md ## Schema de entrada (Parquet do S3)

# COMMAND ----------

# Schema unificado — campos comuns a usage e cost records
INPUT_SCHEMA = StructType([
    StructField("product",       StringType(),    True),
    StructField("date",          StringType(),    True),
    StructField("org_name",      StringType(),    True),
    StructField("metric_name",   StringType(),    True),
    StructField("value",         DoubleType(),    True),
    StructField("unit",          StringType(),    True),
    StructField("extracted_at",  StringType(),    True),
    # Campos exclusivos de cost records
    StructField("product_name",  StringType(),    True),
    StructField("charge_type",   StringType(),    True),
    StructField("cost_usd",      DoubleType(),    True),
    StructField("month",         StringType(),    True),
])

# COMMAND ----------

# MAGIC %md ## Auto Loader — Ingestão Incremental

# COMMAND ----------

# Filtro de produto (se especificado)
source_path = S3_SOURCE_PATH
if PRODUCT_FILTER != "all":
    source_path = f"{S3_SOURCE_PATH}product={PRODUCT_FILTER}/"

print(f"Lendo de: {source_path}")

raw_stream = (
    spark.readStream
    .format("cloudFiles")                      # Auto Loader
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.partitionColumns", "product")
    .schema(INPUT_SCHEMA)
    .load(source_path)
)

# COMMAND ----------

# Normalizar campos: usage records usam 'value', cost records usam 'cost_usd'
bronze_df = (
    raw_stream
    .withColumn(
        "value",
        F.coalesce(
            F.col("value"),
            F.col("cost_usd")
        )
    )
    .withColumn(
        "metric_name",
        F.coalesce(
            F.col("metric_name"),
            F.col("product_name")
        )
    )
    .withColumn(
        "date",
        F.coalesce(
            F.col("date"),
            F.col("month")
        )
    )
    .withColumn("extracted_at",  F.to_timestamp("extracted_at"))
    .withColumn("_source_file",  F.input_file_name())
    .withColumn("_ingested_at",  F.current_timestamp())
    .select(
        "product", "date", "org_name", "metric_name",
        "value", "unit", "extracted_at",
        "_source_file", "_ingested_at"
    )
    .filter(F.col("value").isNotNull() & (F.col("value") > 0))
)

# COMMAND ----------

# MAGIC %md ## Gravar na tabela Bronze

# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
    .option("mergeSchema", "true")
    .partitionBy("product", "date")
    .trigger(availableNow=True)              # Processa todos os arquivos novos e para
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md ## Verificação pós-ingestão

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
print(f"✅ Total de registros na Bronze: {bronze_count:,}")

display(
    spark.table(BRONZE_TABLE)
    .groupBy("product", "date")
    .agg(
        F.count("*").alias("registros"),
        F.sum("value").alias("valor_total"),
        F.max("_ingested_at").alias("ultima_ingestao"),
    )
    .orderBy("product", F.desc("date"))
    .limit(30)
)
