# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2ebadde7-0a64-43ff-85a2-24b2c205ef37",
# META       "default_lakehouse_name": "gld_vigencia",
# META       "default_lakehouse_workspace_id": "591a0c0e-f09f-45b5-b7a9-26f68534a05d",
# META       "known_lakehouses": [
# META         {
# META           "id": "29ed4f8f-72cc-4511-879f-e632ed6ec4f9"
# META         },
# META         {
# META           "id": "2ebadde7-0a64-43ff-85a2-24b2c205ef37"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 06 - Gold: Vigencia Release Communications
# 
# ## Objetivo
# Poblar el hecho `gld_vigencia.fact_deprecation_impact` (compatible con el modelo
# semántico `default` y el reporte `inventory`) a partir de entidades Silver.
# 
# ## Fuentes (Silver)
# - `slv_vigencia.slv_deprecation_event`
# - `slv_vigencia.slv_impacted_technology`
# - `slv_vigencia.slv_asset`
# - `slv_vigencia.slv_asset_technology`
# 
# ## Tabla Gold producida
# - `gld_vigencia.fact_deprecation_impact`
# 
# ## Compatibilidad del modelo
# Este notebook respeta exactamente las columnas esperadas por la fact table:
# - `date_sk`, `asset_sk`, `deprecation_sk`, `technology_sk`
# - `risk_band_sk`, `risk_score`, `is_affected`, `days_to_retirement`
# - `priority`, `remediation_status`, `snapshot_date`, `loaded_at`

# MARKDOWN ********************

# ## Parámetros y helpers

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import datetime

spark.conf.set("spark.sql.caseSensitive", "true")

SILVER_DB = "slv_vigencia"
GOLD_DB = "gld_vigencia"
SNAPSHOT_DATE = datetime.date.today().isoformat()
DATE_SK = int(SNAPSHOT_DATE.replace("-", ""))

TBL_SLV_EVENT = f"{SILVER_DB}.slv_deprecation_event"
TBL_SLV_IMPACTED = f"{SILVER_DB}.slv_impacted_technology"
TBL_SLV_ASSET = f"{SILVER_DB}.slv_asset"
TBL_SLV_ASSET_TECH = f"{SILVER_DB}.slv_asset_technology"
TBL_GLD_DIM_TECH = f"{GOLD_DB}.dim_technology"
TBL_GLD_FACT = f"{GOLD_DB}.fact_deprecation_impact"

print(f"Snapshot date: {SNAPSHOT_DATE}")
print(f"Silver DB: {SILVER_DB}")
print(f"Gold DB: {GOLD_DB}")


def table_exists(t):
    try:
        spark.table(t).limit(1).count()
        return True
    except Exception:
        return False


def write_gold_merge(df, table_name, merge_keys):
    source_df = df.dropDuplicates(merge_keys)

    if not table_exists(table_name):
        (
            source_df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
        print(f"[CREATE] {table_name} -> {source_df.count()} filas")
        return

    target = DeltaTable.forName(spark, table_name)
    cond = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
    upd = {c: f"s.{c}" for c in source_df.columns if c not in merge_keys}
    (
        target.alias("t")
        .merge(source_df.alias("s"), cond)
        .whenMatchedUpdate(set=upd)
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[MERGE]  {table_name} -> completado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validación de dependencias

# CELL ********************

required_tables = [TBL_SLV_EVENT, TBL_SLV_IMPACTED, TBL_SLV_ASSET, TBL_SLV_ASSET_TECH, TBL_GLD_DIM_TECH]
missing = [t for t in required_tables if not table_exists(t)]
if missing:
    raise RuntimeError(f"Faltan tablas requeridas: {missing}")

slv_event = spark.table(TBL_SLV_EVENT)
slv_impacted = spark.table(TBL_SLV_IMPACTED)
slv_asset = spark.table(TBL_SLV_ASSET)
slv_asset_tech = spark.table(TBL_SLV_ASSET_TECH)
dim_tech = spark.table(TBL_GLD_DIM_TECH)

if slv_event.limit(1).count() == 0:
    raise RuntimeError(f"Sin datos en {TBL_SLV_EVENT}")

print(f"slv_deprecation_event   : {slv_event.count():,}")
print(f"slv_impacted_technology : {slv_impacted.count():,}")
print(f"slv_asset               : {slv_asset.count():,}")
print(f"slv_asset_technology    : {slv_asset_tech.count():,}")
print(f"dim_technology          : {dim_tech.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Correlación activo x deprecación

# CELL ********************

dep = (
    slv_event
    .select(
        "deprecation_sk",
        "technology_family",
        "affected_runtime",
        "affected_version",
        "retirement_at",
        "severity",
        "status",
    )
    .dropDuplicates(["deprecation_sk"])
)

dep_tech = (
    slv_impacted
    .select(
        "deprecation_sk",
        "technology_family",
        "affected_runtime",
        "affected_version",
    )
    .dropDuplicates(["deprecation_sk", "technology_family", "affected_runtime", "affected_version"])
)

asset = slv_asset.select("asset_sk", "native_resource_id")

asset_tech = (
    slv_asset_tech
    .select(
        "resource_id",
        "technology_family",
        "runtime_name",
        "runtime_version",
    )
    .dropDuplicates(["resource_id", "technology_family", "runtime_name", "runtime_version"])
)

join_cond = (
    (F.lower(dep_tech["technology_family"]) == F.lower(asset_tech["technology_family"]))
    & (
        dep_tech["affected_runtime"].isNull()
        | (F.trim(dep_tech["affected_runtime"]) == "")
        | (F.lower(dep_tech["affected_runtime"]) == F.lower(asset_tech["runtime_name"]))
    )
    & (
        dep_tech["affected_version"].isNull()
        | (F.trim(dep_tech["affected_version"]) == "")
        | (F.lower(dep_tech["affected_version"]) == F.lower(asset_tech["runtime_version"]))
    )
)

matches = (
    dep_tech.alias("d")
    .join(asset_tech.alias("a"), join_cond, "inner")
    .join(asset.alias("s"), F.col("s.native_resource_id") == F.col("a.resource_id"), "inner")
    .join(dep.select("deprecation_sk", "retirement_at", "severity", "status"), on="deprecation_sk", how="left")
    .select(
        F.col("s.asset_sk"),
        F.col("d.deprecation_sk"),
        F.col("a.technology_family"),
        F.col("a.runtime_name"),
        F.col("a.runtime_version"),
        F.col("retirement_at"),
        F.col("severity"),
        F.col("status"),
    )
    .dropDuplicates(["asset_sk", "deprecation_sk", "technology_family", "runtime_name", "runtime_version"])
)

print(f"Matches activo/deprecación: {matches.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enriquecimiento de riesgo y llaves de modelo

# CELL ********************

dim_tech_norm = (
    dim_tech
    .select(
        "technology_sk",
        F.lower(F.col("technology_family")).alias("technology_family_l"),
        F.lower(F.col("runtime_name")).alias("runtime_name_l"),
        F.lower(F.col("runtime_version")).alias("runtime_version_l"),
    )
    .dropDuplicates(["technology_family_l", "runtime_name_l", "runtime_version_l"])
)

fact_base = (
    matches
    .withColumn("days_to_retirement", F.datediff(F.to_date(F.col("retirement_at")), F.current_date()))
    .withColumn(
        "priority",
        F.when(F.lower(F.col("severity")) == "critical", F.lit("critical"))
        .when(F.lower(F.col("severity")) == "high", F.lit("high"))
        .when(F.lower(F.col("severity")) == "medium", F.lit("medium"))
        .otherwise(F.lit("low")),
    )
    .withColumn(
        "risk_band_sk",
        F.when((F.col("days_to_retirement").isNull()) & (F.lower(F.col("priority")) == "critical"), F.lit("1"))
        .when((F.col("days_to_retirement") <= 30) | (F.lower(F.col("priority")) == "critical"), F.lit("1"))
        .when((F.col("days_to_retirement") <= 90) | (F.lower(F.col("priority")) == "high"), F.lit("2"))
        .when((F.col("days_to_retirement") <= 180) | (F.lower(F.col("priority")) == "medium"), F.lit("3"))
        .otherwise(F.lit("4")),
    )
    .withColumn(
        "risk_score",
        F.when(F.col("risk_band_sk") == "1", F.lit(1.0))
        .when(F.col("risk_band_sk") == "2", F.lit(0.75))
        .when(F.col("risk_band_sk") == "3", F.lit(0.5))
        .when(F.col("risk_band_sk") == "4", F.lit(0.25))
        .otherwise(F.lit(0.0)),
    )
    .withColumn("is_affected", F.lit(True))
    .withColumn("remediation_status", F.lit("pendiente"))
)

fact_gold = (
    fact_base.alias("f")
    .join(
        dim_tech_norm.alias("dt"),
        (F.lower(F.col("f.technology_family")) == F.col("dt.technology_family_l"))
        & (F.lower(F.col("f.runtime_name")) == F.col("dt.runtime_name_l"))
        & (F.lower(F.col("f.runtime_version")) == F.col("dt.runtime_version_l")),
        "left",
    )
    .withColumn("technology_sk", F.coalesce(F.col("dt.technology_sk"), F.sha2(F.concat_ws("|", F.col("f.technology_family"), F.col("f.runtime_name"), F.col("f.runtime_version")), 256)))
    .select(
        F.lit(DATE_SK).alias("date_sk"),
        F.col("f.asset_sk").alias("asset_sk"),
        F.col("f.deprecation_sk").alias("deprecation_sk"),
        F.col("technology_sk"),
        F.col("f.risk_band_sk").alias("risk_band_sk"),
        F.col("f.risk_score").cast("double").alias("risk_score"),
        F.col("f.is_affected").cast("boolean").alias("is_affected"),
        F.col("f.days_to_retirement").cast("int").alias("days_to_retirement"),
        F.col("f.priority").alias("priority"),
        F.col("f.remediation_status").alias("remediation_status"),
        F.lit(SNAPSHOT_DATE).alias("snapshot_date"),
        F.current_timestamp().alias("loaded_at"),
    )
    .dropDuplicates(["date_sk", "asset_sk", "deprecation_sk", "technology_sk"])
)

write_gold_merge(
    fact_gold,
    TBL_GLD_FACT,
    merge_keys=["date_sk", "asset_sk", "deprecation_sk", "technology_sk"],
)

print(f"[STATE] {TBL_GLD_FACT}: {spark.table(TBL_GLD_FACT).count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Vista rápida para validación de reporte

# CELL ********************

display(
    spark.table(TBL_GLD_FACT)
    .select(
        "snapshot_date",
        "risk_band_sk",
        "priority",
        "days_to_retirement",
        "is_affected",
    )
    .orderBy(F.asc("days_to_retirement"))
    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
