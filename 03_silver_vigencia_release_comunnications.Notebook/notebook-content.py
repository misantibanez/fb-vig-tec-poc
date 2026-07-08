# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "29ed4f8f-72cc-4511-879f-e632ed6ec4f9",
# META       "default_lakehouse_name": "slv_vigencia",
# META       "default_lakehouse_workspace_id": "591a0c0e-f09f-45b5-b7a9-26f68534a05d",
# META       "known_lakehouses": [
# META         {
# META           "id": "42a3fa5c-8d4e-452c-ad22-b0569a2b1a6b"
# META         },
# META         {
# META           "id": "29ed4f8f-72cc-4511-879f-e632ed6ec4f9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 03 - Silver: Vigencia Release Communications
# 
# ## Objetivo
# Conformar deprecaciones desde `brz_vigencia.brz_deprecation_raw` hacia entidades Silver.
# 
# ## Fuentes (Bronze)
# - `brz_vigencia.brz_deprecation_raw`
# 
# ## Tablas Silver producidas
# | Tabla | Contenido |
# |---|---|
# | `slv_vigencia.slv_deprecation_event` | Evento técnico normalizado |
# | `slv_vigencia.slv_source_notice` | Aviso fuente estandarizado |
# | `slv_vigencia.slv_impacted_technology` | Tecnología impactada por aviso |
# 
# ## Reglas
# - Deduplicar por `vendor + notice_id` (última versión del aviso)
# - Normalizar estado, severidad e impacto
# - MERGE (upsert) preservando `first_seen_at`

# MARKDOWN ********************

# ## Parámetros y convenciones

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.caseSensitive", "true")

BRONZE_DB = "brz_vigencia"
SILVER_DB = "slv_vigencia"
SOURCE_SYSTEM = "interbank_vigencia_pipeline"

BRONZE_TABLE = f"{BRONZE_DB}.brz_deprecation_raw"
TBL_DEPRECATION_EVENT = f"{SILVER_DB}.slv_deprecation_event"
TBL_SOURCE_NOTICE = f"{SILVER_DB}.slv_source_notice"
TBL_IMPACTED_TECH = f"{SILVER_DB}.slv_impacted_technology"

print(f"Bronze: {BRONZE_DB}")
print(f"Silver: {SILVER_DB}")
print(f"Fuente: {BRONZE_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helpers Silver

# CELL ********************

def table_exists(table_name):
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def write_silver_merge(df, table_name, merge_keys, update_exclude=None):
    update_exclude = update_exclude or []
    source_df = df.dropDuplicates(merge_keys)
    update_cols = [c for c in source_df.columns if c not in merge_keys and c not in update_exclude]

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
    cond = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    (
        target.alias("target")
        .merge(source_df.alias("source"), cond)
        .whenMatchedUpdate(set={c: f"source.{c}" for c in update_cols})
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[MERGE]  {table_name} -> completado")


def normalize_status(col):
    lcol = F.lower(F.trim(col))
    return (
        F.when(lcol.isNull() | (lcol == ""), F.lit("unknown"))
        .when(lcol.contains("retired"), F.lit("retired"))
        .when(lcol.contains("retire"), F.lit("retiring"))
        .when(lcol.contains("deprecated"), F.lit("deprecated"))
        .when(lcol.contains("preview"), F.lit("preview"))
        .otherwise(lcol)
    )


def derive_technology_family(affected_service, affected_runtime, title):
    haystack = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(affected_service, F.lit("")),
            F.coalesce(affected_runtime, F.lit("")),
            F.coalesce(title, F.lit("")),
        )
    )
    return (
        F.when(haystack.rlike("aks|kubernetes|container"), F.lit("containers"))
        .when(haystack.rlike("function|app service|web app|vm|virtual machine"), F.lit("compute"))
        .when(haystack.rlike("sql|mysql|postgres|cosmos|database"), F.lit("data"))
        .when(haystack.rlike("storage|blob|files"), F.lit("storage"))
        .when(haystack.rlike("key vault|identity|entra|aad"), F.lit("security"))
        .when(haystack.rlike("network|gateway|load balancer|dns"), F.lit("network"))
        .otherwise(F.lit("platform"))
    )


def derive_impact_scope(affected_service, affected_feature, affected_runtime, affected_version):
    return (
        F.when(F.col(affected_runtime).isNotNull() & (F.trim(F.col(affected_runtime)) != ""), F.lit("runtime"))
        .when(F.col(affected_feature).isNotNull() & (F.trim(F.col(affected_feature)) != ""), F.lit("feature"))
        .when(F.col(affected_version).isNotNull() & (F.trim(F.col(affected_version)) != ""), F.lit("runtime"))
        .when(F.col(affected_service).isNotNull() & (F.trim(F.col(affected_service)) != ""), F.lit("service"))
        .otherwise(F.lit("configuration"))
    )


def derive_severity(norm_status_col, days_to_retirement_col):
    return (
        F.when(norm_status_col == "retired", F.lit("critical"))
        .when(days_to_retirement_col.isNull(), F.lit("medium"))
        .when(days_to_retirement_col <= 30, F.lit("critical"))
        .when(days_to_retirement_col <= 90, F.lit("high"))
        .when(days_to_retirement_col <= 180, F.lit("medium"))
        .otherwise(F.lit("low"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Carga y deduplicación de Bronze

# CELL ********************

if not table_exists(BRONZE_TABLE):
    raise RuntimeError(f"No existe la tabla fuente: {BRONZE_TABLE}")

brz_df = spark.table(BRONZE_TABLE)
if brz_df.limit(1).count() == 0:
    raise RuntimeError(f"La tabla fuente está vacía: {BRONZE_TABLE}")

dedup_w = Window.partitionBy("vendor", "notice_id").orderBy(
    F.col("published_at").desc_nulls_last(),
    F.col("collected_at").desc_nulls_last(),
)

base_df = (
    brz_df
    .filter(F.col("notice_id").isNotNull())
    .withColumn("rn", F.row_number().over(dedup_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("published_at_ts", F.coalesce(F.col("published_at").cast("timestamp"), F.to_timestamp("published_at")))
    .withColumn("retirement_at_ts", F.coalesce(F.col("retirement_at").cast("timestamp"), F.to_timestamp("retire_date"), F.to_timestamp("retirement_at")))
    .withColumn("status_norm", normalize_status(F.col("status")))
    .withColumn("days_to_retirement", F.datediff(F.to_date(F.col("retirement_at_ts")), F.current_date()))
)

print(f"Bronze deduplicado: {base_df.count():,} avisos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla 1: slv_deprecation_event

# CELL ********************

slv_deprecation_event_df = (
    base_df
    .withColumn("deprecation_sk", F.sha2(F.concat_ws("|", F.col("vendor"), F.col("notice_id")), 256))
    .withColumn("technology_family", derive_technology_family(F.col("affected_service"), F.col("affected_runtime"), F.col("title")))
    .withColumn("impact_scope", derive_impact_scope("affected_service", "affected_feature", "affected_runtime", "affected_version"))
    .withColumn("severity", derive_severity(F.col("status_norm"), F.col("days_to_retirement")))
    .select(
        "deprecation_sk",
        F.col("vendor"),
        F.col("notice_id"),
        F.col("title").alias("notice_title"),
        "technology_family",
        "affected_service",
        "affected_feature",
        "affected_runtime",
        "affected_version",
        F.col("published_at_ts").alias("published_at"),
        F.col("retirement_at_ts").alias("retirement_at"),
        "severity",
        "impact_scope",
        F.col("status_norm").alias("status"),
        F.lit(SOURCE_SYSTEM).alias("source_system"),
        F.current_timestamp().alias("first_seen_at"),
        F.current_timestamp().alias("last_seen_at"),
        F.current_timestamp().alias("conformance_at"),
    )
)

write_silver_merge(
    slv_deprecation_event_df,
    TBL_DEPRECATION_EVENT,
    merge_keys=["deprecation_sk"],
    update_exclude=["first_seen_at"],
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla 2: slv_source_notice

# CELL ********************

slv_source_notice_df = (
    base_df
    .withColumn("notice_sk", F.sha2(F.concat_ws("|", F.col("vendor"), F.col("notice_id")), 256))
    .select(
        "notice_sk",
        F.col("vendor"),
        F.col("notice_id"),
        F.col("title").alias("notice_title"),
        F.lit(None).cast("string").alias("notice_url"),
        "source_system",
        "source_path",
        F.col("published_at_ts").alias("published_at"),
        F.col("retirement_at_ts").alias("retirement_at"),
        F.col("status_norm").alias("status"),
        F.current_timestamp().alias("first_seen_at"),
        F.current_timestamp().alias("last_seen_at"),
        F.current_timestamp().alias("conformance_at"),
    )
)

write_silver_merge(
    slv_source_notice_df,
    TBL_SOURCE_NOTICE,
    merge_keys=["vendor", "notice_id"],
    update_exclude=["first_seen_at"],
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla 3: slv_impacted_technology

# CELL ********************

slv_impacted_technology_df = (
    slv_deprecation_event_df
    .withColumn("technology_nk", F.concat_ws("|", F.coalesce(F.col("affected_service"), F.lit("na")), F.coalesce(F.col("affected_runtime"), F.lit("na")), F.coalesce(F.col("affected_version"), F.lit("na"))))
    .withColumn("technology_sk", F.sha2(F.col("technology_nk"), 256))
    .select(
        "technology_sk",
        "technology_nk",
        "technology_family",
        "affected_service",
        "affected_feature",
        "affected_runtime",
        "affected_version",
        "deprecation_sk",
        F.col("published_at"),
        F.col("retirement_at"),
        "status",
        F.current_timestamp().alias("first_seen_at"),
        F.current_timestamp().alias("last_seen_at"),
        F.current_timestamp().alias("conformance_at"),
    )
)

write_silver_merge(
    slv_impacted_technology_df,
    TBL_IMPACTED_TECH,
    merge_keys=["deprecation_sk", "technology_nk"],
    update_exclude=["first_seen_at"],
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validación rápida

# CELL ********************

for tbl in [TBL_DEPRECATION_EVENT, TBL_SOURCE_NOTICE, TBL_IMPACTED_TECH]:
    cnt = spark.table(tbl).count()
    print(f"[STATE] {tbl}: {cnt:,} filas")

display(
    spark.table(TBL_DEPRECATION_EVENT)
    .select("published_at", "retirement_at", "severity", "status", "notice_title")
    .orderBy(F.desc("published_at"))
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
