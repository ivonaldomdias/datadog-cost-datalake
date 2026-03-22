# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver — Limpeza, Tipagem e Enriquecimento
# MAGIC
# MAGIC Transforma os dados brutos da camada Bronze em dados limpos,
# MAGIC tipados e enriquecidos com campos derivados prontos para análise.
# MAGIC
# MAGIC **Fonte:** `datadog_costs.bronze_raw`
# MAGIC **Destino:** `datadog_costs.silver_costs`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

S3_BUCKET    = dbutils.widgets.get("s3_bucket") if dbutils.widgets.get("s3_bucket") else spark.conf.get("s3_bucket")
BRONZE_TABLE = "datadog_costs.bronze_raw"
SILVER_TABLE = "datadog_costs.silver_costs"

# COMMAND ----------

# MAGIC %md ## Criar Tabela Silver

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    product             STRING      COMMENT 'Produto Datadog',
    date                DATE        COMMENT 'Data de referência',
    year                INT         COMMENT 'Ano (derivado)',
    month               INT         COMMENT 'Mês (derivado)',
    week_of_year        INT         COMMENT 'Semana do ano (derivado)',
    day_of_week         STRING      COMMENT 'Dia da semana (derivado)',
    org_name            STRING      COMMENT 'Organização Datadog',
    metric_name         STRING      COMMENT 'Nome da métrica',
    metric_category     STRING      COMMENT 'Categoria da métrica (usage / cost)',
    value               DOUBLE      COMMENT 'Valor da métrica',
    value_normalized    DOUBLE      COMMENT 'Valor normalizado (GB para logs, K hosts para infra)',
    unit                STRING      COMMENT 'Unidade original',
    unit_normalized     STRING      COMMENT 'Unidade após normalização',
    is_cost_metric      BOOLEAN     COMMENT 'True se a métrica representa custo em USD',
    extracted_at        TIMESTAMP   COMMENT 'Timestamp de extração',
    _silver_updated_at  TIMESTAMP   COMMENT 'Última atualização na Silver'
)
USING DELTA
PARTITIONED BY (product, year, month)
COMMENT 'Camada Silver: dados limpos, tipados e enriquecidos do Datadog'
LOCATION 's3://{S3_BUCKET}/datadog-costs/delta/silver/'
""")

print(f"✅ Tabela {SILVER_TABLE} pronta")

# COMMAND ----------

# MAGIC %md ## Transformações

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

# Mapeamento: métrica → categoria e flag de custo
COST_METRICS = {
    "infrastructure", "apm", "logs", "synthetics",
    "custom_metrics", "estimated_cost"
}

silver_df = (
    bronze_df
    # ── Tipagem de datas ───────────────────────────────────────────────────
    .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    .withColumn("year",         F.year("date"))
    .withColumn("month",        F.month("date"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week",
        F.date_format("date", "EEEE")         # Monday, Tuesday...
    )

    # ── Normalização de valores ────────────────────────────────────────────
    .withColumn(
        "value_normalized",
        F.when(
            F.col("unit") == "bytes",
            F.round(F.col("value") / 1_073_741_824, 4)   # bytes → GB
        ).when(
            F.col("unit") == "hosts",
            F.round(F.col("value") / 1_000, 4)            # hosts → K hosts
        ).otherwise(
            F.round(F.col("value"), 4)
        )
    )
    .withColumn(
        "unit_normalized",
        F.when(F.col("unit") == "bytes", F.lit("GB"))
        .when(F.col("unit") == "hosts",  F.lit("K hosts"))
        .otherwise(F.col("unit"))
    )

    # ── Enriquecimento ─────────────────────────────────────────────────────
    .withColumn(
        "metric_category",
        F.when(F.col("product") == "estimated_cost", F.lit("cost"))
        .otherwise(F.lit("usage"))
    )
    .withColumn(
        "is_cost_metric",
        F.col("product") == "estimated_cost"
    )

    # ── Limpeza ───────────────────────────────────────────────────────────
    .withColumn("org_name",    F.trim(F.lower("org_name")))
    .withColumn("metric_name", F.trim(F.lower("metric_name")))
    .withColumn("product",     F.trim(F.lower("product")))

    # ── Filtros de qualidade ───────────────────────────────────────────────
    .filter(F.col("date").isNotNull())
    .filter(F.col("value").isNotNull() & (F.col("value") >= 0))

    # ── Metadata ──────────────────────────────────────────────────────────
    .withColumn("_silver_updated_at", F.current_timestamp())

    .select(
        "product", "date", "year", "month", "week_of_year", "day_of_week",
        "org_name", "metric_name", "metric_category",
        "value", "value_normalized", "unit", "unit_normalized",
        "is_cost_metric", "extracted_at", "_silver_updated_at"
    )
)

# COMMAND ----------

# MAGIC %md ## Merge (Upsert) na Silver — evita duplicatas

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, f"s3://{S3_BUCKET}/datadog-costs/delta/silver/"):
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)

    (
        silver_table.alias("target")
        .merge(
            silver_df.alias("source"),
            """
            target.product     = source.product AND
            target.date        = source.date    AND
            target.org_name    = source.org_name AND
            target.metric_name = source.metric_name
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("✅ Merge (upsert) concluído na Silver")
else:
    silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
    print("✅ Carga inicial da Silver concluída")

# COMMAND ----------

# MAGIC %md ## Verificação

# COMMAND ----------

silver_count = spark.table(SILVER_TABLE).count()
print(f"✅ Total de registros na Silver: {silver_count:,}")

display(
    spark.table(SILVER_TABLE)
    .groupBy("product", "year", "month")
    .agg(
        F.count("*").alias("registros"),
        F.round(F.sum(F.when(F.col("is_cost_metric"), F.col("value"))), 2).alias("custo_usd"),
        F.countDistinct("metric_name").alias("metricas_distintas"),
    )
    .orderBy(F.desc("year"), F.desc("month"), "product")
)
