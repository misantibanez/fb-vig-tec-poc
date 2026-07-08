# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Vigencia Tecnológica Interbank — Notebook Master en Markdown
# 
# > **Objetivo:** diseñar una arquitectura medallion en Microsoft Fabric para consolidar inventario multicloud, anuncios de deprecación, estándares tecnológicos y riesgo de impacto/remediación.
# 
# ---
# 
# ## 1. Contexto de negocio
# 
# Interbank necesita una capacidad de análisis continuo para:
# 
# - identificar recursos desplegados en Azure y otras nubes;
# - correlacionarlos con anuncios de deprecación, retiro o cambio de soporte;
# - comparar el estado real de los activos contra los estándares tecnológicos del banco;
# - priorizar remediaciones con trazabilidad operativa.
# 
# ### Problemas que resuelve
# 
# - Un anuncio técnico llega por correo, web o documentación oficial.
# - El equipo debe entender qué recursos internos están afectados.
# - El impacto debe medirse por activo, versión, feature y criticidad.
# - El seguimiento debe quedar disponible en reportes y tableros.
# 
# ### Ejemplos de impacto
# 
# - Azure Key Vault: retirar `Access Policies` y migrar a RBAC.
# - Azure Functions: versionado de Python / Node / .NET fuera de soporte.
# - AKS / runtimes / node images: versiones obsoletas o no estándar.
# - AWS / GCP: servicios equivalentes con soporte por versión o feature.
# 
# ---
# 
# ## 2. Principios de diseño
# 
# ### 2.1 Medallion architecture
# 
# | Capa | Propósito | Regla |
# |---|---|---|
# | Bronze | Persistir el dato crudo | No transformar semántica |
# | Silver | Estandarizar y correlacionar | Normalizar, deduplicar, conformar |
# | Gold | Publicación analítica | Modelo estrella / warehouse |
# 
# ### 2.2 Tipos de fuentes
# 
# | Dominio | Ejemplos |
# |---|---|
# | Inventario | Azure Resource Graph, AWS Config, GCP Asset Inventory, CMDB interna |
# | Deprecaciones | Microsoft Learn, AWS docs, Google Cloud docs, emails de fabricante |
# | Estándares | Lineamientos Interbank, políticas corporativas, baseline de versiones |
# | Contexto | Dueños, tags, criticidad, ambientes, fechas de revisión |
# 
# ### 2.3 Entidades de negocio
# 
# - **Asset**: recurso cloud individual.
# - **Technology**: runtime, versión, OS, engine, feature.
# - **Deprecation Event**: anuncio de retiro o cambio de soporte.
# - **Standard**: versión o baseline corporativo.
# - **Risk**: impacto calculado y prioridad de remediación.
# 
# ---
# 
# ## 3. Modelo conceptual
# 
# ### 3.1 Ejes de análisis
# 
# 1. **Qué tengo**: inventario real.
# 2. **Qué cambia**: anuncios y retiros.
# 3. **Qué debo cumplir**: estándar Interbank.
# 4. **Qué se afecta**: impacto y priorización.
# 
# ### 3.2 Relaciones principales


# CELL ********************

Cloud Account -> Asset -> Asset Technology -> Deprecation Match -> Risk \-> Standard Compliance
Deprecation Notice -> Technology Family / Runtime / Feature

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### 3.3 Granularidad sugerida
# 
# - Inventario: 1 fila por recurso por snapshot.
# - Tecnología: 1 fila por recurso por tecnología detectada.
# - Deprecación: 1 fila por anuncio.
# - Match: 1 fila por combinación activo-anuncio.
# - Riesgo: 1 fila por activo-anuncio-fecha.
# 
# ---
# 
# ## 4. Notebook 00 — Parámetros y convenciones
# 
# ### Objetivo
# 
# Centralizar rutas, nombres de tablas, y parámetros de ejecución.
# 
# ### Parámetros mínimos
# 
# - `lakehouse_bronze`
# - `lakehouse_silver`
# - `warehouse_gold`
# - `source_system`
# - `load_date`
# - `cloud_provider`
# - `snapshot_date`
# 
# ### Ejemplo

# CELL ********************

from pyspark.sql import functions as F

BRONZE_DB = "brz_vigencia"
SILVER_DB = "slv_vigencia"
GOLD_DB = "gld_vigencia"

SOURCE_SYSTEM = "interbank_vigencia_pipeline"
SNAPSHOT_DATE = F.current_date()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# ## 5. Notebook 01 — Bronze: inventario multicloud
# 
# ### Objetivo
# 
# Ingerir el inventario crudo de Azure, AWS y GCP sin perder trazabilidad.
# 
# ### Tablas Bronze sugeridas
# 
# | Tabla | Contenido |
# |---|---|
# | `brz_inventory_asset_raw` | Activos crudos por snapshot |
# | `brz_inventory_cloud_raw` | Cuenta / subscription / project / org |
# | `brz_inventory_tag_raw` | Tags o labels por activo |
# | `brz_inventory_configuration_raw` | Configuración y properties JSON |
# 
# ### Esquema mínimo de `brz_inventory_asset_raw`
# 
# - `ingestion_id`
# - `source_system`
# - `cloud_provider`
# - `account_id`
# - `asset_id`
# - `asset_name`
# - `asset_type`
# - `asset_region`
# - `resource_group`
# - `subscription_or_project_id`
# - `tags_json`
# - `properties_json`
# - `collected_at`
# - `raw_payload`
# 
# ### Step by step
# 
# 1. Leer la fuente nativa o export.
# 2. Asignar `ingestion_id`.
# 3. Guardar el payload original en JSON.
# 4. Persistir en Delta sin transformar semántica.
# 5. Registrar fecha de carga y origen.
# 
# ### Pseudocódigo

# CELL ********************

raw_df = spark.read.json(input_path)

bronze_df = (
    raw_df
    .withColumn("ingestion_id", F.expr("uuid()"))
    .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    .withColumn("cloud_provider", F.lit("azure"))
    .withColumn("collected_at", F.current_timestamp())
    .withColumn("raw_payload", F.to_json(F.struct("*")))
)

bronze_df.write.format("delta").mode("append").saveAsTable("brz_inventory_asset_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### Reglas
# 
# - No deduplicar en Bronze.
# - No normalizar versiones todavía.
# - No eliminar columnas.
# - Todo cambio debe ser reversible.
# 
# ---
# 
# ## 6. Notebook 02 — Bronze: anuncios y deprecaciones
# 
# ### Objetivo
# 
# Capturar notificaciones de proveedores y documentos oficiales.
# 
# ### Tablas Bronze sugeridas
# 
# | Tabla | Contenido |
# |---|---|
# | `brz_notice_raw` | Email / web / doc oficial / ticket |
# | `brz_deprecation_raw` | Anuncio técnico estructurado |
# | `brz_standard_raw` | Norma interna de versión objetivo |
# 
# ### Esquema mínimo de `brz_deprecation_raw`
# 
# - `ingestion_id`
# - `source_system`
# - `vendor`
# - `notice_id`
# - `title`
# - `summary`
# - `affected_service`
# - `affected_feature`
# - `affected_runtime`
# - `affected_version`
# - `retirement_at`
# - `published_at`
# - `severity`
# - `raw_payload`
# 
# ### Step by step
# 
# 1. Extraer el texto o JSON del aviso.
# 2. Clasificar proveedor: Microsoft / AWS / GCP / interno.
# 3. Identificar service family, runtime, feature y versión.
# 4. Guardar el raw payload.
# 
# ### Ejemplo de clasificador


# CELL ********************

def classify_notice(row):
    text = (row["title"] + " " + row["summary"]).lower()
    if "key vault" in text:
        return "azure.key_vault"
    if "python" in text and "function" in text:
        return "azure.functions.python"
    return "unclassified"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# ## 7. Notebook 03 — Silver: conformar inventario
# 
# ### Objetivo
# 
# Convertir el inventario crudo en una entidad empresarial consistente.
# 
# ### Tablas Silver sugeridas
# 
# | Tabla | Contenido |
# |---|---|
# | `slv_cloud_account` | Cuenta / subscription / project normalizada |
# | `slv_asset` | Activo empresarial conformado |
# | `slv_asset_technology` | Tecnología detectada por activo |
# | `slv_asset_configuration` | Configuración normalizada |
# 
# ### Definición de `slv_asset`
# 
# - `asset_sk`
# - `asset_nk`
# - `cloud_provider`
# - `account_id`
# - `subscription_id`
# - `project_id`
# - `resource_group`
# - `asset_name`
# - `asset_type`
# - `region`
# - `environment`
# - `criticality`
# - `owner_team`
# - `first_seen_at`
# - `last_seen_at`
# 
# ### Step by step
# 
# 1. Estandarizar nombres de columnas.
# 2. Resolver `cloud_provider`.
# 3. Derivar `asset_nk` con reglas de negocio.
# 4. Normalizar regiones y ambientes.
# 5. Deduplicar por key natural.
# 
# ### Regla sugerida para `asset_nk`

# CELL ********************

asset_nk = cloud_provider + '|' + account_id + '|' + asset_type + '|' + normalized_asset_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### Tecnología por activo
# 
# Detectar:
# 
# - runtime
# - versión
# - framework
# - OS
# - engine
# - SKU
# 
# Ejemplo:

# CELL ********************

Azure Functions -> python 3.8
AKS -> node image version
Key Vault -> access policy mode
App Service -> .NET version

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# ## 8. Notebook 04 — Silver: conformar deprecaciones
# 
# ### Objetivo
# 
# Convertir anuncios dispersos en eventos analíticos comparables.
# 
# ### Tablas Silver sugeridas
# 
# | Tabla | Contenido |
# |---|---|
# | `slv_deprecation_event` | Evento técnico normalizado |
# | `slv_impacted_technology` | Tecnología impactada por evento |
# | `slv_source_notice` | Documento o aviso fuente |
# 
# ### Definición de `slv_deprecation_event`
# 
# - `deprecation_sk`
# - `vendor`
# - `notice_id`
# - `notice_title`
# - `technology_family`
# - `affected_service`
# - `affected_feature`
# - `affected_runtime`
# - `affected_version`
# - `published_at`
# - `retirement_at`
# - `severity`
# - `impact_scope`
# - `status`
# 
# ### Step by step
# 
# 1. Normalizar nombres de runtime y versiones.
# 2. Convertir fechas a tipo date/timestamp.
# 3. Clasificar severidad.
# 4. Asignar `impact_scope`.
# 5. Consolidar duplicados de un mismo anuncio.
# 
# ### Impact scope sugerido
# 
# - `service`
# - `feature`
# - `runtime`
# - `os`
# - `sku`
# - `configuration`
# 
# ---
# 
# ## 9. Notebook 05 — Silver: correlación de impacto
# 
# ### Objetivo
# 
# Cruzar inventario con anuncios y estándares.
# 
# ### Tablas Silver sugeridas
# 
# | Tabla | Contenido |
# |---|---|
# | `slv_asset_deprecation_match` | Match activo-anuncio |
# | `slv_asset_standard_gap` | Brecha contra estándar |
# | `slv_asset_risk` | Riesgo final calculado |
# | `slv_remediation_candidate` | Candidatos de remediación |
# 
# ### Lógica de matching
# 
# Un activo puede impactarse por:
# 
# - `resource_type`
# - `technology_family`
# - `runtime_name`
# - `runtime_version`
# - `feature_flag`
# - `cloud_provider`
# 
# ### Score sugerido
# 
# | Criterio | Peso |
# |---|---|
# | Match exacto de runtime | Alto |
# | Match de feature | Alto |
# | Match de service family | Medio |
# | Match parcial de versión | Medio |
# | Match por heurística textual | Bajo |
# 
# ### Ejemplo de resultado
# 
# | asset | deprecation | score | affected | days_to_retirement |
# |---|---|---:|---|---:|
# | kv-001 | Key Vault Access Policies | 0.95 | yes | 120 |
# | func-101 | Python 3.8 EOL | 0.98 | yes | 45 |
# 
# ### Step by step
# 
# 1. Hacer join por family / runtime / feature.
# 2. Calcular score de similitud.
# 3. Determinar si el activo está afectado.
# 4. Calcular días hasta retiro.
# 5. Clasificar riesgo.
# 
# ### Bandas de riesgo
# 
# - `critical`
# - `high`
# - `medium`
# - `low`
# 
# ---
# 
# ## 10. Notebook 06 — Gold: warehouse analítico
# 
# ### Objetivo
# 
# Publicar un modelo dimensional listo para Power BI y tableros de operación.
# 
# ### Dimensiones recomendadas
# 
# - `dim_cloud_provider`
# - `dim_account`
# - `dim_subscription`
# - `dim_asset`
# - `dim_technology`
# - `dim_deprecation_notice`
# - `dim_standard`
# - `dim_date`
# - `dim_severity`
# - `dim_risk_band`
# - `dim_environment`
# 
# ### Hechos recomendados
# 
# - `fact_inventory_snapshot`
# - `fact_deprecation_impact`
# - `fact_remediation_progress`
# - `fact_risk_exposure`
# 
# ### Ejemplo de `fact_deprecation_impact`
# 
# - `date_sk`
# - `asset_sk`
# - `deprecation_sk`
# - `risk_score`
# - `is_affected`
# - `days_to_retirement`
# - `priority`
# - `remediation_status`
# 
# ### Step by step
# 
# 1. Tomar silver como fuente.
# 2. Cargar dimensiones.
# 3. Generar surrogate keys.
# 4. Poblar hechos.
# 5. Validar integridad referencial.
# 
# ### KPI de negocio
# 
# - activos afectados hoy
# - activos afectados en 30/60/90 días
# - brechas por estándar
# - riesgos por nube / servicio / equipo
# - avance de remediación
# 
# ---
# 
# ## 11. Notebook 07 — Validación y calidad
# 
# ### Objetivo
# 
# Garantizar consistencia del modelo.
# 
# ### Validaciones mínimas
# 
# 1. Unicidad de `asset_nk`.
# 2. No null en keys empresariales.
# 3. Match entre inventario y estándares.
# 4. Anuncios sin clasificación.
# 5. Activos sin dueño o criticidad.
# 6. Fechas de retiro válidas.
# 
# ### Ejemplo


# CELL ********************

assets = spark.table("slv_asset")
dup = assets.groupBy("asset_nk").count().filter("count > 1")
assert dup.count() == 0, "asset_nk duplicado"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### Validaciones de impacto

# CELL ********************

risk = spark.table("slv_asset_risk")
critical = risk.filter("risk_band = 'critical'")
critical.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# ## 12. Modelo empresarial final
# 
# ### Flujo completo

# CELL ********************

Bronze
  -> inventario crudo
  -> anuncios crudos
  -> estándares crudos

Silver
  -> activos conformados
  -> eventos de deprecación
  -> matches
  -> riesgo

Gold
  -> warehouse
  -> KPIs
  -> tableros
  -> seguimiento operativo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### Preguntas que debe responder el modelo
# 
# - ¿Qué activos están expuestos a una deprecación?
# - ¿Cuándo vence el soporte?
# - ¿Qué norma interna incumplen?
# - ¿Qué equipo debe remediar?
# - ¿Qué impacto hay por nube, región y servicio?
# 
# ---
# 
# ## 13. Siguiente paso recomendado
# 
# Convertir este diseño en:
# 
# 1. notebooks PySpark reales para Bronze, Silver y Gold;
# 2. esquemas Delta listos para Fabric;
# 3. modelo estrella para Warehouse;
# 4. dashboard de riesgo y cumplimiento;
# 5. pipeline de carga incremental.
# 
# ---
# 
# ## 14. Glosario corto
# 
# | Término | Significado |
# |---|---|
# | **Deprecation** | Función, runtime o feature que será retirada o dejada de soportar |
# | **Runtime** | Entorno de ejecución de una app o function |
# | **Asset** | Recurso cloud individual |
# | **Standard** | Regla interna de versión mínima/objetivo |
# | **Bronze** | Dato crudo |
# | **Silver** | Dato conformado |
# | **Gold** | Dato analítico |
# 
# ---
# 
# *Documento base para el diseño de vigencia tecnológica en Interbank sobre Microsoft Fabric.*

