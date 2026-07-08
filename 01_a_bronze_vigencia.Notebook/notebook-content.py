# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "42a3fa5c-8d4e-452c-ad22-b0569a2b1a6b",
# META       "default_lakehouse_name": "brz_vigencia",
# META       "default_lakehouse_workspace_id": "591a0c0e-f09f-45b5-b7a9-26f68534a05d",
# META       "known_lakehouses": [
# META         {
# META           "id": "42a3fa5c-8d4e-452c-ad22-b0569a2b1a6b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01_a_bronze_vigencia
# Procesa el landing de `00_a_azure-release-comunication-retirements` y persiste
# el dataset de obsolescencia en Bronze (`brz_vigencia.brz_deprecation_raw`).

# CELL ********************

from pyspark.sql import functions as F

spark.conf.set("spark.sql.caseSensitive", "true")

BRONZE_DB = "brz_vigencia"
SOURCE_SYSTEM = "interbank_vigencia_pipeline"
CLOUD_PROVIDER = "azure"
VENDOR = "microsoft"

RELEASE_COMM_SOURCE_PATH = (
    "abfss://Interbank@onelake.dfs.fabric.microsoft.com/"
    "brz_vigencia.Lakehouse/Files/obsolecencia/azure/release_communications"
)

TARGET_TABLE = f"{BRONZE_DB}.brz_deprecation_raw"

print("SOURCE_PATH =", RELEASE_COMM_SOURCE_PATH)
print("TARGET_TABLE =", TARGET_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helpers Bronze

# CELL ********************

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def read_release_communications(path: str):
    return (
        spark.read
        .option("multiLine", True)
        .option("recursiveFileLookup", "true")
        .json(path)
        .withColumn("source_path", F.input_file_name())
    )


def filter_unprocessed_sources(df, table_name: str):
    if not table_exists(table_name):
        return df
    existing_sources = spark.table(table_name).select("source_path").distinct()
    return df.join(existing_sources, on="source_path", how="left_anti")


def build_brz_deprecation_raw(df):
    source_cols = list(df.columns)

    return (
        df
        .withColumn("ingestion_id", F.expr("uuid()"))
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("cloud_provider", F.lit(CLOUD_PROVIDER))
        .withColumn("vendor", F.lit(VENDOR))
        .withColumn("notice_id", F.sha2(F.concat_ws("|", F.col("source_path"), F.col("created"), F.col("title")), 256))
        .withColumn("title", F.col("title"))
        .withColumn("summary", F.lit(None).cast("string"))
        .withColumn("affected_service", F.lit("azure.release_communications"))
        .withColumn("affected_feature", F.lit(None).cast("string"))
        .withColumn("affected_runtime", F.lit(None).cast("string"))
        .withColumn("affected_version", F.lit(None).cast("string"))
        .withColumn("retirement_at", F.to_timestamp("retire_date"))
        .withColumn("published_at", F.to_timestamp("created"))
        .withColumn("severity", F.lit(None).cast("string"))
        .withColumn("collected_at", F.current_timestamp())
        .withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in source_cols])))
        .select(
            "ingestion_id",
            "source_system",
            "cloud_provider",
            "vendor",
            "notice_id",
            "title",
            "summary",
            "affected_service",
            "affected_feature",
            "affected_runtime",
            "affected_version",
            "retirement_at",
            "published_at",
            "severity",
            "status",
            "retire_date",
            "retire_year",
            "collected_at",
            "source_path",
            "raw_payload",
        )
    )


def write_bronze(df, table_name: str):
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Carga, transformación y persistencia

# CELL ********************

source_df = read_release_communications(RELEASE_COMM_SOURCE_PATH)
if source_df.limit(1).count() == 0:
    raise RuntimeError(f"No se encontraron archivos JSON en: {RELEASE_COMM_SOURCE_PATH}")

new_source_df = filter_unprocessed_sources(source_df, TARGET_TABLE)
new_files = new_source_df.select("source_path").distinct().count()

if new_files == 0:
    print("[SKIP] No hay nuevos archivos para procesar (source_path ya cargados).")
else:
    bronze_df = build_brz_deprecation_raw(new_source_df)
    write_bronze(bronze_df, TARGET_TABLE)
    print(f"[DONE] Archivos nuevos procesados: {new_files}")
    print(f"[DONE] Filas insertadas: {bronze_df.count()}")

final_df = spark.table(TARGET_TABLE)
print(f"[STATE] {TARGET_TABLE}: {final_df.count()} filas totales")
display(
    final_df
    .select("published_at", "retirement_at", "status", "title", "source_path")
    .orderBy(F.desc("published_at"))
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
