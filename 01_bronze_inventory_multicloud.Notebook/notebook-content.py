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

# # 01 - Bronze: Inventario Azure
# 
# ## Objetivo
# Ingerir el inventario crudo de Azure en formato Bronze, preservando el payload original y la trazabilidad de carga.
# 
# ## Alcance
# - Recursos Azure por snapshot
# - Subscription / resource group / ubicacion
# - Tags (key-value estructurados)
# - Configuracion cruda en JSON
# - Campos de tecnologia / runtime por tipo de recurso
# - Metadata de ingestion
# 
# ## Regla
# Bronze no transforma semantica. Solo persiste lo que llega.

# MARKDOWN ********************

# ## Parametros y convenciones

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
spark.conf.set('spark.sql.caseSensitive', 'true')

BRONZE_DB = 'brz_vigencia'
SOURCE_SYSTEM = 'interbank_vigencia_pipeline'
CLOUD_PROVIDER = 'azure'

AZURE_INVENTORY_ONELAKE_BASE_PATH = (
    'abfss://Interbank@onelake.dfs.fabric.microsoft.com/'
    'brz_vigencia.Lakehouse/Files/vigencia/inventory/azure'
)

AZURE_INVENTORY_SOURCE_PATH = [AZURE_INVENTORY_ONELAKE_BASE_PATH]

print('BRONZE_DB =', BRONZE_DB)
print('SOURCE_SYSTEM =', SOURCE_SYSTEM)
print('CLOUD_PROVIDER =', CLOUD_PROVIDER)
print('AZURE_INVENTORY_SOURCE_PATH =', AZURE_INVENTORY_SOURCE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tablas Bronze
# 
# | Tabla | Contenido |
# |---|---|
# | brz_azure_resource_raw | JSON crudo del inventario Azure |
# | brz_azure_resource_flat | Vista plana con campos top-level, runtime y configuracion |
# | brz_inventory_tag_raw | Tags por activo como key-value |
# | brz_inventory_cloud_raw | Suscripciones detectadas en el snapshot |

# MARKDOWN ********************

# ## Control de cargas Bronze
# Re-ejecuciones del mismo dia para el mismo source_path deben REFRESCAR la porcion diaria,
# no saltarse. Asi evitamos tablas desfasadas cuando resources.json cambia en el dia.

# CELL ********************

def table_exists(table_name):
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def write_bronze(df, table_name, source_path=None, mode="append"):
    # Modo acumulado: cada snapshot versionado se agrega.
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(table_name)
    print(f"[APPEND] {table_name} -> {df.count()} filas escritas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helpers de carga e ingesta

# CELL ********************

def read_azure_inventory_json(path):
    return (
        spark.read
        .option('multiLine', True)
        .option('recursiveFileLookup', 'true')
        .json(path)
    )


def add_bronze_metadata(df, source_path=None, cloud_provider=CLOUD_PROVIDER):
    has_source_col = 'source_path' in df.columns
    source_expr = F.col('source_path') if has_source_col else F.lit(source_path)

    payload_cols = [c for c in df.columns if c not in {'ingestion_id', 'source_system', 'cloud_provider', 'collected_at', 'source_path', 'raw_payload'}]

    return (
        df
        .withColumn('ingestion_id', F.expr('uuid()'))
        .withColumn('source_system', F.lit(SOURCE_SYSTEM))
        .withColumn('cloud_provider', F.lit(cloud_provider))
        .withColumn('collected_at', F.current_timestamp())
        .withColumn('source_path', source_expr)
        .withColumn('raw_payload', F.to_json(F.struct(*[F.col(c) for c in payload_cols])))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extraccion de campos planos y de tecnologia
# 
# Se extraen campos de tecnologia por tipo de recurso usando get_json_object
# sobre properties para manejar el schema variable de Azure sin fallos de inferencia.
# 
# | Recurso | Campos extraidos |
# |---|---|
# | Functions y App Service | linuxFxVersion, windowsFxVersion, Node, Python, .NET, Java, PHP, PowerShell |
# | AKS | kubernetesVersion, currentKubernetesVersion |
# | Key Vault | enableRbacAuthorization, enableSoftDelete, softDeleteRetentionInDays |
# | VM | vmSize, osType, imageOffer, imageSku, imageVersion |
# | SQL y Databases | serviceObjective, collation, version |
# | Container Registry | loginServer |
# | Storage Account | accessTier, minimumTlsVersion |

# CELL ********************

def flatten_azure_inventory(df):
    props = F.to_json(F.col('properties'))
    return (
        df
        .select(
            F.col('id').alias('resource_id'),
            F.col('name').alias('resource_name'),
            F.lower(F.col('type')).alias('resource_type'),
            F.col('subscription_id'),
            F.col('resource_group'),
            F.col('location'),
            F.col('kind'),
            F.col('sku.name').alias('sku_name'),
            F.col('sku.tier').alias('sku_tier'),
            F.to_json(F.col('tags')).alias('tags_json'),
            F.to_json(F.col('properties')).alias('properties_json'),
            F.col('properties.provisioningState').alias('provisioning_state'),
            F.col('properties.state').alias('state'),
            F.col('properties.description').alias('description'),
            F.get_json_object(props, '$.siteConfig.linuxFxVersion').alias('runtime_linux_fx_version'),
            F.get_json_object(props, '$.siteConfig.windowsFxVersion').alias('runtime_windows_fx_version'),
            F.get_json_object(props, '$.siteConfig.nodeVersion').alias('runtime_node_version'),
            F.get_json_object(props, '$.siteConfig.pythonVersion').alias('runtime_python_version'),
            F.get_json_object(props, '$.siteConfig.netFrameworkVersion').alias('runtime_dotnet_version'),
            F.get_json_object(props, '$.siteConfig.javaVersion').alias('runtime_java_version'),
            F.get_json_object(props, '$.siteConfig.phpVersion').alias('runtime_php_version'),
            F.get_json_object(props, '$.siteConfig.powerShellVersion').alias('runtime_powershell_version'),
            F.get_json_object(props, '$.kubernetesVersion').alias('aks_kubernetes_version'),
            F.get_json_object(props, '$.currentKubernetesVersion').alias('aks_kubernetes_version_current'),
            F.get_json_object(props, '$.nodeResourceGroup').alias('aks_node_resource_group'),
            F.get_json_object(props, '$.enableRbacAuthorization').alias('kv_rbac_enabled'),
            F.get_json_object(props, '$.enableSoftDelete').alias('kv_soft_delete_enabled'),
            F.get_json_object(props, '$.softDeleteRetentionInDays').alias('kv_soft_delete_retention_days'),
            F.get_json_object(props, '$.hardwareProfile.vmSize').alias('vm_size'),
            F.get_json_object(props, '$.storageProfile.osDisk.osType').alias('vm_os_type'),
            F.get_json_object(props, '$.storageProfile.imageReference.offer').alias('vm_image_offer'),
            F.get_json_object(props, '$.storageProfile.imageReference.sku').alias('vm_image_sku'),
            F.get_json_object(props, '$.storageProfile.imageReference.version').alias('vm_image_version'),
            F.get_json_object(props, '$.currentServiceObjectiveName').alias('sql_service_objective'),
            F.get_json_object(props, '$.collation').alias('sql_collation'),
            F.get_json_object(props, '$.version').alias('db_version'),
            F.get_json_object(props, '$.loginServer').alias('acr_login_server'),
            F.get_json_object(props, '$.accessTier').alias('storage_access_tier'),
            F.get_json_object(props, '$.minimumTlsVersion').alias('storage_minimum_tls_version'),
            F.col('source_path'),
            F.col('collected_at'),
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extraccion de tags estructuradas
# Explota el mapa de tags en filas (resource_id, tag_key, tag_value) para brz_inventory_tag_raw.
# Permite filtrar activos por tag en Silver sin parsear JSON.

# CELL ********************

def extract_tags_raw(df):
    return (
        df
        .select(
            'ingestion_id',
            'source_system',
            'cloud_provider',
            'collected_at',
            'source_path',
            F.col('id').alias('resource_id'),
            F.col('subscription_id'),
            F.from_json(
                F.to_json(F.col('tags')),
                MapType(StringType(), StringType())
            ).alias('tags_map')
        )
        .filter(F.col('tags_map').isNotNull())
        .select(
            'ingestion_id',
            'source_system',
            'cloud_provider',
            'collected_at',
            'source_path',
            'resource_id',
            'subscription_id',
            F.explode('tags_map').alias('tag_key', 'tag_value')
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extraccion de cuentas cloud (suscripciones)
# Genera brz_inventory_cloud_raw con las suscripciones unicas del snapshot.
# Sirve como catalogo de cuentas para enriquecer Silver con metadata de negocio.

# CELL ********************

def extract_cloud_accounts(df):
    return (
        df
        .select('subscription_id')
        .distinct()
        .withColumn('ingestion_id', F.expr('uuid()'))
        .withColumn('source_system', F.lit(SOURCE_SYSTEM))
        .withColumn('cloud_provider', F.lit(CLOUD_PROVIDER))
        .withColumn('account_type', F.lit('azure_subscription'))
        .withColumn('collected_at', F.current_timestamp())
        .withColumn('source_path', F.lit(AZURE_INVENTORY_ONELAKE_BASE_PATH))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Carga del inventario Azure
# Lee resources.json desde OneLake y agrega metadata de ingesta.

# CELL ********************

def load_azure_inventory_sources(source_path):
    df = read_azure_inventory_json(source_path)
    # Cada fila conserva el archivo origen (snapshot) para trazabilidad y dedupe posterior.
    df = df.withColumn('source_path', F.input_file_name())
    df = add_bronze_metadata(df, source_path)
    return df


raw_source_df = load_azure_inventory_sources(AZURE_INVENTORY_SOURCE_PATH[0])

print('Registros cargados:', raw_source_df.count())
raw_source_df.printSchema()
raw_source_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Persistencia Bronze
# Escribe cuatro tablas Delta con control de duplicados por source_path y fecha de carga.

# CELL ********************

source_path = AZURE_INVENTORY_SOURCE_PATH[0]

bronze_raw_df = raw_source_df.select(
    'ingestion_id', 'source_system', 'cloud_provider', 'collected_at', 'source_path', 'raw_payload'
)
write_bronze(bronze_raw_df, f'{BRONZE_DB}.brz_azure_resource_raw', source_path)

bronze_flat_df = flatten_azure_inventory(raw_source_df)
write_bronze(bronze_flat_df, f'{BRONZE_DB}.brz_azure_resource_flat', source_path)

bronze_tags_df = extract_tags_raw(raw_source_df)
write_bronze(bronze_tags_df, f'{BRONZE_DB}.brz_inventory_tag_raw', source_path)

bronze_accounts_df = extract_cloud_accounts(raw_source_df)
write_bronze(bronze_accounts_df, f'{BRONZE_DB}.brz_inventory_cloud_raw', source_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checklist de validacion
# 
# - [ ] ingestion_id presente en todas las tablas
# - [ ] source_system y cloud_provider correctos
# - [ ] raw_payload no nulo en brz_azure_resource_raw
# - [ ] Campos runtime presentes en brz_azure_resource_flat
# - [ ] Tags explotadas en brz_inventory_tag_raw
# - [ ] Suscripciones en brz_inventory_cloud_raw
# - [ ] Sin duplicados por re-ejecucion del mismo dia

# CELL ********************

import matplotlib.pyplot as plt

df_plot = raw_source_df.select(F.lower(F.col("type")).alias("resource_type"))
top_n = 15
pdf = (
    df_plot
    .filter(F.col("resource_type").isNotNull())
    .groupBy("resource_type")
    .count()
    .orderBy(F.desc("count"))
    .limit(top_n)
    .toPandas()
)

if pdf.empty:
    print("No hay datos para graficar.")
else:
    pdf = pdf.sort_values("count", ascending=True)
    plt.figure(figsize=(12, 7))
    plt.barh(pdf["resource_type"], pdf["count"])
    plt.title(f"Top {top_n} tipos de recursos Azure")
    plt.xlabel("Cantidad")
    plt.ylabel("Resource Type")
    plt.tight_layout()
    plt.show()
    print(f"Total recursos analizados: {int(pdf['count'].sum())} (top {top_n})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Siguiente notebook
# El siguiente paso es 02_bronze_deprecations_sources para anuncios, emails y estandares.
