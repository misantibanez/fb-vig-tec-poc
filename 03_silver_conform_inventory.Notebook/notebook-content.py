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

# # 03 - Silver: Conformar Inventario
# 
# ## Objetivo
# Transformar el inventario Bronze en entidades empresariales normalizadas.
# 
# ## Fuentes (Bronze)
# - brz_vigencia.brz_azure_resource_flat
# - brz_vigencia.brz_inventory_tag_raw
# - brz_vigencia.brz_inventory_cloud_raw
# 
# ## Tablas Silver producidas
# | Tabla | Contenido |
# |---|---|
# | slv_vigencia.slv_cloud_account | Suscripcion normalizada |
# | slv_vigencia.slv_asset | Activo empresarial conformado |
# | slv_vigencia.slv_asset_technology | Tecnologia detectada por activo |
# 
# ## Reglas
# - Deduplicar por clave natural (asset_nk)
# - Normalizar regiones, ambientes y tipos
# - MERGE (upsert) para preservar first_seen_at y actualizar last_seen_at

# MARKDOWN ********************

# ## Parametros y convenciones

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
from delta.tables import DeltaTable

spark.conf.set('spark.sql.caseSensitive', 'true')

BRONZE_DB     = 'brz_vigencia'
SILVER_DB     = 'slv_vigencia'
SOURCE_SYSTEM  = 'interbank_vigencia_pipeline'
CLOUD_PROVIDER = 'azure'

print(f'Bronze: {BRONZE_DB}')
print(f'Silver: {SILVER_DB}')

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

    # Evita MERGE ambiguo: una sola fila fuente por clave de merge.
    source_df = df.dropDuplicates(merge_keys)

    all_cols = [c for c in source_df.columns if c not in merge_keys and c not in update_exclude]

    if not table_exists(table_name):
        (source_df.write.format("delta")
           .option("mergeSchema", "true")
           .mode("overwrite")
           .saveAsTable(table_name))
        print(f"[CREATE] {table_name} -> {source_df.count()} filas")
        return

    target = DeltaTable.forName(spark, table_name)
    condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    (target.alias("target")
           .merge(source_df.alias("source"), condition)
           .whenMatchedUpdate(set={c: f"source.{c}" for c in all_cols})
           .whenNotMatchedInsertAll()
           .execute())
    print(f"[MERGE]  {table_name} -> completado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Carga de fuentes Bronze

# CELL ********************

brz_flat     = spark.table(f'{BRONZE_DB}.brz_azure_resource_flat')
brz_tags     = spark.table(f'{BRONZE_DB}.brz_inventory_tag_raw')
brz_accounts = spark.table(f'{BRONZE_DB}.brz_inventory_cloud_raw')

print(f'brz_azure_resource_flat : {brz_flat.count():,} filas')
print(f'brz_inventory_tag_raw   : {brz_tags.count():,} filas')
print(f'brz_inventory_cloud_raw : {brz_accounts.count():,} filas')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla 1: slv_cloud_account
# Una fila por suscripcion unica. Ambiente derivado del nombre de suscripcion.

# CELL ********************

def normalize_account_env(col):
    return (
        F.when(F.lower(col).contains('prod'), 'production')
         .when(F.lower(col).contains('dev'),  'development')
         .when(F.lower(col).contains('test'), 'testing')
         .when(F.lower(col).contains('stg'),  'staging')
         .when(F.lower(col).contains('staging'), 'staging')
         .otherwise('unknown')
    )


def build_slv_cloud_account(df):
    latest = df.groupBy("subscription_id").agg(F.max("collected_at").alias("last_seen_at"))
    return (
        df
        .join(latest, on="subscription_id", how="inner")
        .dropDuplicates(["subscription_id"])
        .select(
            F.expr("uuid()").alias("account_sk"),
            F.col("subscription_id").alias("account_nk"),
            F.lit(CLOUD_PROVIDER).alias('cloud_provider'),
            F.col("account_type"),
            normalize_account_env(F.col("subscription_id")).alias("environment"),
            F.lit(SOURCE_SYSTEM).alias('source_system'),
            F.current_timestamp().alias("first_seen_at"),
            F.current_timestamp().alias("last_seen_at"),
            F.current_timestamp().alias("conformance_at"),
        )
    )


slv_account_df = build_slv_cloud_account(brz_accounts)
slv_account_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla 2: slv_asset
# Una fila por activo (deduplicado por asset_nk).
# asset_nk = cloud_provider|subscription_id|resource_type|resource_name_normalizado
# Enriquecido con ambiente, criticidad y equipo dueno desde tags.

# CELL ********************

REGION_MAP = {
    'eastus': 'East US', 'eastus2': 'East US 2',
    'westus': 'West US', 'westus2': 'West US 2',
    'centralus': 'Central US', 'southcentralus': 'South Central US',
    'northcentralus': 'North Central US', 'brazilsouth': 'Brazil South',
    'eastasia': 'East Asia', 'southeastasia': 'Southeast Asia',
    'westeurope': 'West Europe', 'northeurope': 'North Europe',
    'uksouth': 'UK South', 'ukwest': 'UK West', 'australiaeast': 'Australia East',
}

def normalize_region(col):
    expr = col
    for raw, friendly in REGION_MAP.items():
        expr = F.when(F.lower(col) == raw, friendly).otherwise(expr)
    return expr


def pivot_tags(tags_df, tag_keys):
    return (
        tags_df
        .filter(F.lower(F.col("tag_key")).isin([k.lower() for k in tag_keys]))
        .select("resource_id", F.lower(F.col("tag_key")).alias("tag_key"), "tag_value")
        .groupBy("resource_id")
        .pivot("tag_key", [k.lower() for k in tag_keys])
        .agg(F.first("tag_value"))
    )


def build_slv_asset(flat_df, tags_df):
    tag_keys = ["env", "environment", "criticality", "criticidad", "team", "owner", "squad", "app"]
    tags_pivot = pivot_tags(tags_df, tag_keys)
    base = (
        flat_df
        .dropDuplicates(["resource_id"])
        .join(tags_pivot, on="resource_id", how="left")
    )
    cols = base.columns
    asset_nk = F.concat_ws("|",
        F.lit(CLOUD_PROVIDER),
        F.col("subscription_id"),
        F.lower(F.col("resource_type")),
        F.lower(F.trim(F.col("resource_name")))
    )
    env_col  = F.coalesce(
        F.col('environment') if 'environment' in cols else F.lit(None),
        F.col('env')         if 'env'         in cols else F.lit(None),
        F.lit('unknown')
    )
    crit_col = F.coalesce(
        F.col('criticality') if 'criticality' in cols else F.lit(None),
        F.col('criticidad')  if 'criticidad'  in cols else F.lit(None),
        F.lit('unknown')
    )
    owner_col = F.coalesce(
        F.col('owner') if 'owner' in cols else F.lit(None),
        F.col('team')  if 'team'  in cols else F.lit(None),
        F.col('squad') if 'squad' in cols else F.lit(None),
        F.lit('unknown')
    )
    return (
        base.select(
            F.expr("uuid()").alias("asset_sk"),
            asset_nk.alias("asset_nk"),
            F.lit(CLOUD_PROVIDER).alias('cloud_provider'),
            F.col("subscription_id").alias("account_id"),
            F.col("resource_group"),
            F.col("resource_id").alias("native_resource_id"),
            F.col("resource_name").alias("asset_name"),
            F.lower(F.col("resource_type")).alias("asset_type"),
            normalize_region(F.col("location")).alias("region"),
            F.lower(F.trim(env_col)).alias("environment"),
            F.lower(F.trim(crit_col)).alias("criticality"),
            F.lower(F.trim(owner_col)).alias("owner_team"),
            F.col("provisioning_state"),
            F.col("kind"),
            F.col("sku_name"),
            F.col("sku_tier"),
            F.lit(SOURCE_SYSTEM).alias('source_system'),
            F.current_timestamp().alias("first_seen_at"),
            F.current_timestamp().alias("last_seen_at"),
            F.current_timestamp().alias("conformance_at"),
        )
    )


slv_asset_df = build_slv_asset(brz_flat, brz_tags)
print(f'slv_asset: {slv_asset_df.count():,} activos')
slv_asset_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tabla 3: slv_asset_technology
# Una fila por tecnologia detectada en un activo.
# Cubre: Functions/App Service, AKS, Key Vault, VM, SQL, Storage.
# Campos: technology_family, technology_source, runtime_name, runtime_version, feature_flag

# CELL ********************

def build_slv_asset_technology(flat_df):
    base_cols = ["resource_id", "resource_type", "subscription_id", "resource_name"]
    tech_cols  = [
        "runtime_linux_fx_version", "runtime_windows_fx_version",
        "runtime_node_version", "runtime_python_version", "runtime_dotnet_version",
        "runtime_java_version", "runtime_php_version", "runtime_powershell_version",
        "aks_kubernetes_version", "aks_kubernetes_version_current",
        "kv_rbac_enabled", "kv_soft_delete_enabled",
        "vm_size", "vm_os_type", "vm_image_offer", "vm_image_sku",
        "sql_service_objective", "db_version",
        "storage_minimum_tls_version",
    ]
    available = [c for c in base_cols + tech_cols if c in flat_df.columns]
    df = flat_df.select(available).dropDuplicates(["resource_id"])

    def mk(src, family, source, name_expr, ver_expr, flag_expr, filt=None):
        d = src if filt is None else src.filter(filt)
        return (
            d.select(
                *base_cols,
                F.lit(family).alias("technology_family"),
                F.lit(source).alias("technology_source"),
                name_expr.alias("runtime_name"),
                ver_expr.alias("runtime_version"),
                flag_expr.alias("feature_flag"),
            )
            .filter(F.col("runtime_name").isNotNull() | F.col("runtime_version").isNotNull() | F.col("feature_flag").isNotNull())
        )

    null_str = F.lit(None).cast("string")
    sep = r"\|"

    frames = []
    if "runtime_linux_fx_version" in available:
        frames.append(mk(df, "app_runtime", "linux_fx_version",
            F.lower(F.split("runtime_linux_fx_version", sep)[0]),
            F.split("runtime_linux_fx_version", sep)[1],
            null_str,
            F.col("runtime_linux_fx_version").isNotNull()))

    if "runtime_windows_fx_version" in available:
        frames.append(mk(df, "app_runtime", "windows_fx_version",
            F.lower(F.split("runtime_windows_fx_version", sep)[0]),
            F.split("runtime_windows_fx_version", sep)[1],
            null_str,
            F.col("runtime_windows_fx_version").isNotNull()))

    for field, fname in [("runtime_node_version","node"), ("runtime_python_version","python"),
                         ("runtime_dotnet_version","dotnet"), ("runtime_java_version","java"),
                         ("runtime_php_version","php"), ("runtime_powershell_version","powershell")]:
        if field in available:
            frames.append(mk(df, "app_runtime", field,
                F.lit(fname), F.col(field), null_str, F.col(field).isNotNull()))

    if "aks_kubernetes_version" in available:
        frames.append(mk(
            df.filter(F.col("aks_kubernetes_version").isNotNull() | F.col("aks_kubernetes_version_current").isNotNull()),
            "container_orchestration", "kubernetes_version",
            F.lit("kubernetes"),
            F.coalesce(F.col("aks_kubernetes_version_current"), F.col("aks_kubernetes_version")),
            null_str
        ))

    if "kv_rbac_enabled" in available:
        frames.append(mk(df, "key_vault_config", "rbac_authorization",
            F.lit("rbac_authorization"), null_str, F.col("kv_rbac_enabled"),
            F.col("kv_rbac_enabled").isNotNull()))

    if "kv_soft_delete_enabled" in available:
        frames.append(mk(df, "key_vault_config", "soft_delete",
            F.lit("soft_delete"), null_str, F.col("kv_soft_delete_enabled"),
            F.col("kv_soft_delete_enabled").isNotNull()))

    if "vm_os_type" in available:
        frames.append(mk(df, "virtual_machine", "os_image",
            F.lower(F.col("vm_os_type")), F.col("vm_image_sku"), F.col("vm_image_offer"),
            F.col("vm_os_type").isNotNull()))

    if "db_version" in available:
        frames.append(mk(df, "database", "db_version",
            F.lit("sql"), F.col("db_version"), F.col("sql_service_objective"),
            F.col("db_version").isNotNull()))

    if "storage_minimum_tls_version" in available:
        frames.append(mk(df, "storage_config", "tls_version",
            F.lit("tls"), F.col("storage_minimum_tls_version"), null_str,
            F.col("storage_minimum_tls_version").isNotNull()))

    if not frames:
        raise RuntimeError("No se detectaron tecnologias. Verifica Bronze.")

    tech_df = frames[0]
    for f in frames[1:]:
        tech_df = tech_df.union(f)

    return (
        tech_df
        .withColumn("tech_sk",     F.expr("uuid()"))
        .withColumn("asset_nk",    F.concat_ws("|", F.lit(CLOUD_PROVIDER), F.col("subscription_id"),
                                               F.lower(F.col("resource_type")), F.lower(F.trim(F.col("resource_name")))))
        .withColumn("cloud_provider",  F.lit(CLOUD_PROVIDER))
        .withColumn("source_system",   F.lit(SOURCE_SYSTEM))
        .withColumn("conformance_at",  F.current_timestamp())
        .select(
            "tech_sk", "resource_id", "asset_nk", "resource_type",
            "technology_family", "technology_source",
            "runtime_name", "runtime_version", "feature_flag",
            "cloud_provider", "source_system", "conformance_at"
        )
    )


slv_tech_df = build_slv_asset_technology(brz_flat)
print(f'slv_asset_technology: {slv_tech_df.count():,} filas')
slv_tech_df.groupBy("technology_family", "runtime_name").count().orderBy(F.desc("count")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Persistencia Silver (MERGE / upsert)

# CELL ********************

write_silver_merge(
    slv_account_df, f'{SILVER_DB}.slv_cloud_account',
    merge_keys=['account_nk'], update_exclude=['first_seen_at']
)

write_silver_merge(
    slv_asset_df, f'{SILVER_DB}.slv_asset',
    merge_keys=['asset_nk'], update_exclude=['first_seen_at']
)

write_silver_merge(
    slv_tech_df, f'{SILVER_DB}.slv_asset_technology',
    merge_keys=['resource_id', 'technology_family', 'technology_source'],
    update_exclude=[]
)

print('Persistencia Silver completada.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validaciones

# CELL ********************

assets = spark.table(f'{SILVER_DB}.slv_asset')
dups = assets.groupBy("asset_nk").count().filter("count > 1")
dup_count = dups.count()
if dup_count == 0:
    print("[OK] asset_nk: sin duplicados")
else:
    print(f'[WARN] asset_nk: {dup_count} duplicados detectados')
    dups.show(10, truncate=False)

nulls_type = assets.filter(F.col("asset_type").isNull()).count()
nulls_prov = assets.filter(F.col("cloud_provider").isNull()).count()
print(f'[OK] asset_type nulos   : {nulls_type}')
print(f'[OK] cloud_provider nulos: {nulls_prov}')

tech = spark.table(f'{SILVER_DB}.slv_asset_technology')
print(f'Total tecnologias detectadas: {tech.count():,}')
tech.groupBy("technology_family", "runtime_name").count().orderBy(F.desc("count")).show(20, truncate=False)

assets_with_tech = tech.select("resource_id").distinct()
assets_without = assets.join(assets_with_tech,
    assets["native_resource_id"] == assets_with_tech["resource_id"], "left_anti")
print(f'Activos sin tecnologia detectada: {assets_without.count():,}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Siguiente notebook
# 04_silver_deprecations: conformar anuncios de deprecacion.
# 05_silver_impact_matching: cruzar slv_asset_technology con slv_deprecation_event.
