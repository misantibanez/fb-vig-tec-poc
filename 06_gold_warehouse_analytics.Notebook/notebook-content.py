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
# META           "id": "42a3fa5c-8d4e-452c-ad22-b0569a2b1a6b"
# META         },
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

# # 06 - Gold: Warehouse Analitico (Modelo Estrella + KPIs)
# ## Objetivo
# Publicar modelo dimensional listo para Power BI y tableros de operacion.
# ## Fuentes (Silver)
# - slv_vigencia.slv_cloud_account
# - slv_vigencia.slv_asset
# - slv_vigencia.slv_asset_technology
# ## Dimensiones
# | Tabla | Descripcion |
# |---|---|
# | dim_date | Calendario (ultimos 3 anos + proximos 2) |
# | dim_environment | Ambientes conocidos |
# | dim_cloud_provider | Proveedores de nube |
# | dim_account | Suscripciones / cuentas |
# | dim_asset | Activos empresariales |
# | dim_technology | Familias de tecnologia y runtime |
# | dim_risk_band | Bandas de riesgo |
# ## Hechos
# | Tabla | Descripcion |
# |---|---|
# | fact_inventory_snapshot | Un registro por activo por dia de snapshot |
# | fact_deprecation_impact | Stub: se poblara desde notebook 05 |

# MARKDOWN ********************

# ## Parametros

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import datetime

spark.conf.set('spark.sql.caseSensitive', 'true')

SILVER_DB     = 'slv_vigencia'
GOLD_DB       = 'gld_vigencia'
SOURCE_SYSTEM  = 'interbank_vigencia_pipeline'
SNAPSHOT_DATE  = datetime.date.today().isoformat()

print(f'Silver  : {SILVER_DB}')
print(f'Gold    : {GOLD_DB}')
print(f'Snapshot: {SNAPSHOT_DATE}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper: escribir Gold

# CELL ********************

def table_exists(t):
    try:
        spark.table(t).limit(1).count()
        return True
    except Exception:
        return False


def write_gold(df, table_name, merge_keys=None, mode="overwrite"):
    if merge_keys and table_exists(table_name):
        target = DeltaTable.forName(spark, table_name)
        cond   = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        upd    = {c: f"s.{c}" for c in df.columns if c not in merge_keys}
        (target.alias("t")
               .merge(df.alias("s"), cond)
               .whenMatchedUpdate(set=upd)
               .whenNotMatchedInsertAll()
               .execute())
        print(f"[MERGE]    {table_name}")
    else:
        (df.write.format("delta")
           .option("mergeSchema", "true")
           .mode(mode)
           .saveAsTable(table_name))
        print(f"[OVERWRITE] {table_name} -> {df.count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Carga Silver

# CELL ********************

slv_account = spark.table(f'{SILVER_DB}.slv_cloud_account')
slv_asset   = spark.table(f'{SILVER_DB}.slv_asset')
slv_tech    = spark.table(f'{SILVER_DB}.slv_asset_technology')

print(f'slv_cloud_account    : {slv_account.count():,}')
print(f'slv_asset            : {slv_asset.count():,}')
print(f'slv_asset_technology : {slv_tech.count():,}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_date
# Calendario de los ultimos 3 anos + proximos 2 anos.

# CELL ********************

def build_dim_date(start_year=None, end_year=None):
    import datetime
    today = datetime.date.today()
    if start_year is None: start_year = today.year - 3
    if end_year   is None: end_year   = today.year + 2
    start = datetime.date(start_year, 1, 1)
    end   = datetime.date(end_year, 12, 31)
    dates = [start + datetime.timedelta(d) for d in range((end - start).days + 1)]
    rows  = [
        (int(d.strftime("%Y%m%d")), d.isoformat(),
         d.year, (d.month - 1) // 3 + 1, d.month, d.isocalendar()[1],
         d.day, d.weekday(), d.strftime("%A"), d.strftime("%B"), d.weekday() >= 5)
        for d in dates
    ]
    schema = StructType([
        StructField("date_sk",     IntegerType(), False),
        StructField("full_date",   StringType(),  False),
        StructField("year",        IntegerType(), False),
        StructField("quarter",     IntegerType(), False),
        StructField("month",       IntegerType(), False),
        StructField("week",        IntegerType(), False),
        StructField("day",         IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name",    StringType(),  False),
        StructField("month_name",  StringType(),  False),
        StructField("is_weekend",  BooleanType(), False),
    ])
    return spark.createDataFrame(rows, schema)


dim_date_df = build_dim_date()
write_gold(dim_date_df, f'{GOLD_DB}.dim_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_environment

# CELL ********************

env_rows = [
    ("1", "production",  True,  "Ambiente productivo"),
    ("2", "staging",     False, "Pre-productivo / UAT"),
    ("3", "testing",     False, "Ambiente de pruebas"),
    ("4", "development", False, "Desarrollo"),
    ("5", "unknown",     False, "Sin clasificar"),
]
dim_env_schema = StructType([
    StructField("environment_sk",   StringType(),  False),
    StructField("environment_name", StringType(),  False),
    StructField("is_production",    BooleanType(), False),
    StructField("description",      StringType(),  True),
])
dim_env_df = spark.createDataFrame(env_rows, dim_env_schema)
write_gold(dim_env_df, f'{GOLD_DB}.dim_environment')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_cloud_provider

# CELL ********************

cp_rows = [
    ("1", "azure",  "Microsoft Azure",       "https://azure.microsoft.com"),
    ("2", "aws",    "Amazon Web Services",   "https://aws.amazon.com"),
    ("3", "gcp",    "Google Cloud Platform", "https://cloud.google.com"),
    ("4", "onprem", "On-Premises",           None),
]
dim_cp_schema = StructType([
    StructField("provider_sk",   StringType(), False),
    StructField("provider_name", StringType(), False),
    StructField("display_name",  StringType(), True),
    StructField("url",           StringType(), True),
])
dim_cp_df = spark.createDataFrame(cp_rows, dim_cp_schema)
write_gold(dim_cp_df, f'{GOLD_DB}.dim_cloud_provider')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_account

# CELL ********************

dim_account_df = (
    slv_account
    .select(
        F.col("account_sk"),
        F.col("account_nk"),
        F.col("cloud_provider"),
        F.col("account_type"),
        F.col("environment"),
        F.current_timestamp().alias("loaded_at"),
    )
)
write_gold(dim_account_df, f'{GOLD_DB}.dim_account')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_asset

# CELL ********************

from pyspark.sql.window import Window as _W

# Deduplicar por asset_nk tomando la fila con first_seen_at mas antigua
# (evita duplicados de asset_sk cuando Silver fue corrido multiples veces)
_w_asset = _W.partitionBy("asset_nk").orderBy(F.col("first_seen_at").asc_nulls_last())

dim_asset_df = (
    slv_asset
    .withColumn("_rn", F.row_number().over(_w_asset))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        F.col("asset_sk"),
        F.col("asset_nk"),
        F.col("cloud_provider"),
        F.col("account_id"),
        F.col("resource_group"),
        F.col("asset_name"),
        F.col("asset_type"),
        F.col("region"),
        F.col("environment"),
        F.col("criticality"),
        F.col("owner_team"),
        F.col("provisioning_state"),
        F.col("sku_name"),
        F.col("sku_tier"),
        F.col("native_resource_id"),
        F.col("first_seen_at"),
        F.col("last_seen_at"),
        F.current_timestamp().alias("loaded_at"),
    )
)
write_gold(dim_asset_df, f'{GOLD_DB}.dim_asset')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_technology
# Combinaciones unicas de familia / runtime / version detectadas en Silver.

# CELL ********************

dim_technology_df = (
    slv_tech
    .select("technology_family", "technology_source", "runtime_name", "runtime_version")
    .distinct()
    .withColumn("technology_sk",       F.expr("uuid()"))
    .withColumn("is_deprecated_stub",  F.lit(False))
    .withColumn("loaded_at",           F.current_timestamp())
    .select(
        "technology_sk", "technology_family", "technology_source",
        "runtime_name", "runtime_version", "is_deprecated_stub", "loaded_at",
    )
)
write_gold(dim_technology_df, f'{GOLD_DB}.dim_technology')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## dim_risk_band
# Estatica. Se usara en fact_deprecation_impact (notebook 05).

# CELL ********************

risk_rows = [
    ("1", "critical", 4, "Retiro en menos de 30 dias o ya expirado"),
    ("2", "high",     3, "Retiro en 30-90 dias"),
    ("3", "medium",   2, "Retiro en 90-180 dias"),
    ("4", "low",      1, "Retiro en mas de 180 dias o bajo impacto"),
    ("5", "none",     0, "Sin riesgo detectado"),
]
dim_risk_schema = StructType([
    StructField("risk_band_sk",   StringType(),  False),
    StructField("risk_band_name", StringType(),  False),
    StructField("risk_order",     IntegerType(), False),
    StructField("description",    StringType(),  True),
])
dim_risk_df = spark.createDataFrame(risk_rows, dim_risk_schema)
write_gold(dim_risk_df, f'{GOLD_DB}.dim_risk_band')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## fact_inventory_snapshot
# Un registro por activo por snapshot. Permite ver evolucion del inventario.
# Claves: date_sk, asset_sk. Medidas: is_active, has_technology, technology_count.

# CELL ********************

def build_fact_inventory_snapshot(asset_df, tech_df, snapshot_date):
    tech_count = (
        tech_df
        .groupBy("resource_id")
        .agg(
            F.count("*").alias("technology_count"),
            F.to_json(F.collect_set("technology_family")).alias("technology_families_json"),
            F.to_json(F.collect_set("runtime_name")).alias("runtime_names_json"),
        )
    )
    snap_sk = int(snapshot_date.replace("-", ""))
    return (
        asset_df
        .join(tech_count, asset_df["native_resource_id"] == tech_count["resource_id"], "left")
        .select(
            F.lit(snap_sk).alias("date_sk"),
            F.col("asset_sk"),
            F.col("account_id").alias("account_nk"),
            F.col("cloud_provider"),
            F.col("asset_type"),
            F.col("region"),
            F.col("environment"),
            F.col("criticality"),
            F.col("owner_team"),
            F.col("provisioning_state"),
            F.when(F.col("provisioning_state") == "Succeeded", True).otherwise(False).alias("is_active"),
            F.coalesce(F.col("technology_count"), F.lit(0)).alias("technology_count"),
            F.when(F.col("technology_count") > 0, True).otherwise(False).alias("has_technology"),
            F.col("technology_families_json"),
            F.col("runtime_names_json"),
            F.lit(snapshot_date).alias("snapshot_date"),
            F.current_timestamp().alias("loaded_at"),
        )
    )


fact_snap_df = build_fact_inventory_snapshot(slv_asset, slv_tech, SNAPSHOT_DATE)
write_gold(fact_snap_df, f'{GOLD_DB}.fact_inventory_snapshot', merge_keys=['date_sk', 'asset_sk'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## fact_deprecation_impact (stub)
# Se poblara cuando existan slv_deprecation_event y slv_asset_deprecation_match
# generados por notebooks 04 y 05.

# CELL ********************

stub_schema = StructType([
    StructField("date_sk",            IntegerType(),  True),
    StructField("asset_sk",           StringType(),   True),
    StructField("deprecation_sk",     StringType(),   True),
    StructField("technology_sk",      StringType(),   True),
    StructField("risk_band_sk",       StringType(),   True),
    StructField("risk_score",         DoubleType(),   True),
    StructField("is_affected",        BooleanType(),  True),
    StructField("days_to_retirement", IntegerType(),  True),
    StructField("priority",           StringType(),   True),
    StructField("remediation_status", StringType(),   True),
    StructField("snapshot_date",      StringType(),   True),
    StructField("loaded_at",          TimestampType(),True),
])
if not table_exists(f"{GOLD_DB}.fact_deprecation_impact"):
    spark.createDataFrame([], stub_schema).write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.fact_deprecation_impact")
    print("[STUB] fact_deprecation_impact creada vacia. Poblar desde notebook 05.")
else:
    print("[OK]   fact_deprecation_impact ya existe.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## KPIs del inventario

# CELL ********************

print("KPI 1: Activos totales por nube y tipo")
spark.table(f"{GOLD_DB}.dim_asset").groupBy("cloud_provider", "asset_type").count().orderBy(F.desc("count")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("KPI 2: Distribucion por ambiente")
(spark.table(f"{GOLD_DB}.dim_asset")
    .groupBy("environment")
    .agg(
        F.count("*").alias("total_assets"),
        F.countDistinct("owner_team").alias("teams"),
        F.countDistinct("region").alias("regions"),
    )
    .orderBy(F.desc("total_assets"))
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("KPI 3: Tecnologias mas usadas")
(spark.table(f"{GOLD_DB}.dim_asset")
    .join(spark.table(f"{SILVER_DB}.slv_asset_technology"),
          F.col("native_resource_id") == F.col("resource_id"), "left")
    .groupBy("technology_family", "runtime_name", "runtime_version")
    .count()
    .orderBy(F.desc("count"))
    .show(20, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("KPI 4: Activos por criticidad")
(spark.table(f"{GOLD_DB}.fact_inventory_snapshot")
    .groupBy("criticality")
    .agg(
        F.count("*").alias("total_assets"),
        F.sum(F.col("has_technology").cast("int")).alias("con_tecnologia"),
        F.sum(F.when(~F.col("has_technology"), 1).otherwise(0)).alias("sin_tecnologia"),
    )
    .orderBy(F.desc("total_assets"))
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("KPI 5: Activos por region")
spark.table(f"{GOLD_DB}.dim_asset").groupBy("region").count().orderBy(F.desc("count")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("KPI 6: Activos sin tecnologia detectada (candidatos a revision)")
fact = spark.table(f"{GOLD_DB}.fact_inventory_snapshot")
sin_tech = fact.filter(~F.col("has_technology"))
print(f"Total sin tecnologia: {sin_tech.count():,}")
sin_tech.groupBy("asset_type", "environment").count().orderBy(F.desc("count")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("KPI 7: Top equipos duenos")
(spark.table(f"{GOLD_DB}.dim_asset")
    .groupBy("owner_team")
    .agg(
        F.count("*").alias("total_assets"),
        F.countDistinct("asset_type").alias("tipos"),
    )
    .orderBy(F.desc("total_assets"))
    .show(20, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Resumen de tablas Gold

# CELL ********************

gold_tables = [
    "dim_date", "dim_environment", "dim_cloud_provider",
    "dim_account", "dim_asset", "dim_technology", "dim_risk_band",
    "fact_inventory_snapshot", "fact_deprecation_impact",
]
rows = []
for t in gold_tables:
    try:
        n = spark.table(f"{GOLD_DB}.{t}").count()
        rows.append((t, n))
    except Exception as e:
        rows.append((t, -1))

header = f"{'Tabla':<35} {'Filas':>10}"
print(header)
print("-" * 47)
for name, cnt in rows:
    flag = "OK" if cnt >= 0 else "ERROR"
    print(f"{name:<35} {cnt:>10,}  [{flag}]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Siguiente paso
# - Notebook 04: conformar deprecaciones (slv_deprecation_event)
# - Notebook 05: matching activo x deprecacion (slv_asset_deprecation_match, slv_asset_risk)
# - Re-ejecutar este notebook para poblar fact_deprecation_impact con riesgo real.

