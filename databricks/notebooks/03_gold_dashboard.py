# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold — Agregações e Visualizações de Custos Datadog
# MAGIC
# MAGIC Gera tabelas Gold otimizadas para dashboards e análises executivas,
# MAGIC com visualizações nativas do Databricks.
# MAGIC
# MAGIC **Fonte:** `datadog_costs.silver_costs`
# MAGIC **Destinos:**
# MAGIC - `datadog_costs.gold_daily_summary` — Resumo diário por produto
# MAGIC - `datadog_costs.gold_product_trend` — Tendência mensal por produto
# MAGIC - `datadog_costs.gold_top_hosts` — Top hosts de infraestrutura
# MAGIC - `datadog_costs.gold_cost_anomalies` — Anomalias de consumo

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

S3_BUCKET    = dbutils.widgets.get("s3_bucket") if dbutils.widgets.get("s3_bucket") else spark.conf.get("s3_bucket")
SILVER_TABLE = "datadog_costs.silver_costs"
DATABASE     = "datadog_costs"

silver = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md ## 1. Gold — Resumo Diário por Produto

# COMMAND ----------

gold_daily = (
    silver
    .groupBy("product", "date", "year", "month", "day_of_week", "org_name")
    .agg(
        F.round(F.sum(F.when(F.col("is_cost_metric"), F.col("value")).otherwise(0)), 2)
          .alias("cost_usd"),
        F.round(F.sum(F.when(~F.col("is_cost_metric"), F.col("value")).otherwise(0)), 2)
          .alias("usage_total"),
        F.round(F.sum(F.when(~F.col("is_cost_metric"), F.col("value_normalized")).otherwise(0)), 4)
          .alias("usage_normalized"),
        F.first("unit_normalized").alias("usage_unit"),
        F.count("metric_name").alias("metrics_count"),
        F.current_timestamp().alias("_gold_updated_at"),
    )
    .orderBy("product", "date")
)

gold_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "datadog_costs.gold_daily_summary"
)
print(f"✅ gold_daily_summary: {gold_daily.count():,} registros")

# COMMAND ----------

# MAGIC %md ## 2. Gold — Tendência Mensal por Produto

# COMMAND ----------

window_prev_month = Window.partitionBy("product", "org_name").orderBy("year", "month")

gold_trend = (
    silver
    .groupBy("product", "year", "month", "org_name")
    .agg(
        F.round(F.sum(F.when(F.col("is_cost_metric"), F.col("value")).otherwise(0)), 2)
          .alias("cost_usd"),
        F.round(F.avg(F.when(~F.col("is_cost_metric"), F.col("value"))), 2)
          .alias("avg_daily_usage"),
        F.round(F.max(F.when(~F.col("is_cost_metric"), F.col("value"))), 2)
          .alias("peak_usage"),
        F.countDistinct("date").alias("active_days"),
    )
    .withColumn(
        "cost_usd_prev_month",
        F.lag("cost_usd", 1).over(window_prev_month)
    )
    .withColumn(
        "cost_mom_change_pct",
        F.when(
            F.col("cost_usd_prev_month") > 0,
            F.round(
                (F.col("cost_usd") - F.col("cost_usd_prev_month"))
                / F.col("cost_usd_prev_month") * 100, 2
            )
        ).otherwise(None)
    )
    .withColumn(
        "cost_trend",
        F.when(F.col("cost_mom_change_pct") > 10, F.lit("🔺 Alta"))
        .when(F.col("cost_mom_change_pct") < -10, F.lit("🔻 Queda"))
        .otherwise(F.lit("➡️ Estável"))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy("product", F.desc("year"), F.desc("month"))
)

gold_trend.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "datadog_costs.gold_product_trend"
)
print(f"✅ gold_product_trend: {gold_trend.count():,} registros")

# COMMAND ----------

# MAGIC %md ## 3. Gold — Top Hosts de Infraestrutura

# COMMAND ----------

gold_hosts = (
    silver
    .filter(
        (F.col("product") == "infrastructure") &
        (F.col("metric_name").isin("aws_hosts", "azure_hosts", "gcp_hosts", "agent_hosts", "total_hosts"))
    )
    .groupBy("metric_name", "year", "month", "org_name")
    .agg(
        F.round(F.avg("value"), 1).alias("avg_hosts"),
        F.round(F.max("value"), 1).alias("peak_hosts"),
        F.round(F.min("value"), 1).alias("min_hosts"),
        F.countDistinct("date").alias("days_active"),
    )
    .withColumn(
        "cloud_provider",
        F.when(F.col("metric_name").contains("aws"),   F.lit("AWS"))
        .when(F.col("metric_name").contains("azure"),  F.lit("Azure"))
        .when(F.col("metric_name").contains("gcp"),    F.lit("GCP"))
        .when(F.col("metric_name").contains("agent"),  F.lit("On-Prem/Agent"))
        .otherwise(F.lit("Total"))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.desc("year"), F.desc("month"), F.desc("avg_hosts"))
)

gold_hosts.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "datadog_costs.gold_top_hosts"
)
print(f"✅ gold_top_hosts: {gold_hosts.count():,} registros")

# COMMAND ----------

# MAGIC %md ## 4. Gold — Anomalias de Consumo (Z-Score)

# COMMAND ----------

window_stats = Window.partitionBy("product", "metric_name", "month")

gold_anomalies = (
    silver
    .filter(~F.col("is_cost_metric"))
    .withColumn("avg_value",    F.avg("value").over(window_stats))
    .withColumn("stddev_value", F.stddev("value").over(window_stats))
    .withColumn(
        "z_score",
        F.when(
            F.col("stddev_value") > 0,
            F.round((F.col("value") - F.col("avg_value")) / F.col("stddev_value"), 2)
        ).otherwise(0.0)
    )
    .withColumn(
        "is_anomaly",
        F.abs(F.col("z_score")) > 2.0         # Desvio padrão > 2σ
    )
    .withColumn(
        "anomaly_severity",
        F.when(F.abs(F.col("z_score")) > 3.0, F.lit("🔴 Alta"))
        .when(F.abs(F.col("z_score")) > 2.0,  F.lit("🟡 Média"))
        .otherwise(F.lit("🟢 Normal"))
    )
    .filter(F.col("is_anomaly"))
    .select(
        "product", "date", "metric_name", "org_name",
        "value", "avg_value", "stddev_value", "z_score",
        "anomaly_severity", "unit", "_silver_updated_at"
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.desc("date"), F.desc("z_score"))
)

gold_anomalies.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "datadog_costs.gold_cost_anomalies"
)
print(f"✅ gold_cost_anomalies: {gold_anomalies.count():,} anomalias detectadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 Visualizações

# COMMAND ----------

# MAGIC %md ## Dashboard 1 — Custo Mensal por Produto

# COMMAND ----------

trend_pd = (
    spark.table("datadog_costs.gold_product_trend")
    .filter(F.col("cost_usd") > 0)
    .select("product", "year", "month", "cost_usd")
    .toPandas()
)

trend_pd["period"] = trend_pd["year"].astype(str) + "-" + trend_pd["month"].astype(str).str.zfill(2)
pivot = trend_pd.pivot_table(index="period", columns="product", values="cost_usd", aggfunc="sum").fillna(0)
pivot = pivot.sort_index()

fig, ax = plt.subplots(figsize=(14, 6))
pivot.plot(kind="bar", stacked=True, ax=ax, colormap="tab10")
ax.set_title("💰 Custo Mensal por Produto Datadog (USD)", fontsize=14, fontweight="bold")
ax.set_xlabel("Mês")
ax.set_ylabel("Custo (USD)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax.legend(title="Produto", bbox_to_anchor=(1.01, 1), loc="upper left")
ax.tick_params(axis="x", rotation=45)
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md ## Dashboard 2 — Variação MoM (Month-over-Month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product,
# MAGIC     CONCAT(year, '-', LPAD(month, 2, '0')) AS period,
# MAGIC     cost_usd,
# MAGIC     cost_usd_prev_month,
# MAGIC     cost_mom_change_pct,
# MAGIC     cost_trend
# MAGIC FROM datadog_costs.gold_product_trend
# MAGIC WHERE cost_usd > 0
# MAGIC ORDER BY product, period DESC

# COMMAND ----------

# MAGIC %md ## Dashboard 3 — Distribuição de Hosts por Cloud

# COMMAND ----------

hosts_pd = (
    spark.table("datadog_costs.gold_top_hosts")
    .filter(F.col("cloud_provider") != "Total")
    .groupBy("cloud_provider", "year", "month")
    .agg(F.round(F.avg("avg_hosts"), 1).alias("avg_hosts"))
    .orderBy(F.desc("year"), F.desc("month"))
    .limit(50)
    .toPandas()
)

hosts_pd["period"] = hosts_pd["year"].astype(str) + "-" + hosts_pd["month"].astype(str).str.zfill(2)
pivot_hosts = hosts_pd.pivot_table(index="period", columns="cloud_provider", values="avg_hosts").fillna(0)

fig2, ax2 = plt.subplots(figsize=(14, 5))
pivot_hosts.plot(kind="area", stacked=True, ax=ax2, alpha=0.7, colormap="Set2")
ax2.set_title("🖥️ Hosts de Infraestrutura por Cloud Provider", fontsize=14, fontweight="bold")
ax2.set_xlabel("Mês")
ax2.set_ylabel("Hosts (média)")
ax2.legend(title="Cloud", bbox_to_anchor=(1.01, 1))
ax2.tick_params(axis="x", rotation=45)
plt.tight_layout()
display(fig2)

# COMMAND ----------

# MAGIC %md ## Dashboard 4 — Anomalias Detectadas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date,
# MAGIC     product,
# MAGIC     metric_name,
# MAGIC     ROUND(value, 2)       AS value,
# MAGIC     ROUND(avg_value, 2)   AS avg_value,
# MAGIC     ROUND(z_score, 2)     AS z_score,
# MAGIC     anomaly_severity
# MAGIC FROM datadog_costs.gold_cost_anomalies
# MAGIC ORDER BY date DESC, z_score DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md ## Sumário Executivo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CONCAT(year, '-', LPAD(month, 2, '0'))   AS mes,
# MAGIC     ROUND(SUM(cost_usd), 2)                  AS total_custo_usd,
# MAGIC     COUNT(DISTINCT product)                  AS produtos_ativos,
# MAGIC     SUM(active_days)                         AS total_dias_com_dados
# MAGIC FROM datadog_costs.gold_product_trend
# MAGIC WHERE cost_usd > 0
# MAGIC GROUP BY year, month
# MAGIC ORDER BY year DESC, month DESC
# MAGIC LIMIT 12
