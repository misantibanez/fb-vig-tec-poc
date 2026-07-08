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

# # Recomendaciones de retiro de Azure Advisor (Fabric)
# 
# Mismo diseno que `azure-inventory.ipynb`, pero en vez de inventariar recursos, extrae de
# **Azure Advisor** (`advisorresources` via Azure Resource Graph) las recomendaciones cuyo
# `problem`/`solution` mencionan **retiro** (`retir`) o **deprecacion** (`deprecat`), a nivel de
# **todo el tenant** (todas las suscripciones), con **landing raw en OneLake** (JSON).
# 
# **Login identico a `azure-inventory.ipynb`:** Service Principal (`TENANT_ID` / `CLIENT_ID`) con el
# secreto leido de **Key Vault** en runtime (nunca en el codigo) por la workspace identity.
# 
# **Prerequisitos** (scripts .sh ya ejecutados): `01-provisioning-fabric.sh`,
# `02-setup-workspace-identity.sh`, `03-setup-sp-keyvault.sh`.
# 
# **Diseno:** unidad = 1 suscripcion (paralelismo entre subs, paginacion `$skipToken` secuencial
# dentro de cada una), escritura streaming a `.json` -> subida idempotente a OneLake como checkpoint,
# throttling robusto con `x-ms-user-quota-resets-after`. Requiere un **Lakehouse adjunto** al notebook.
# 
# > Nota de cobertura: Advisor solo emite recomendaciones de retiro **por-recurso** para un
# > subconjunto de servicios. Para el catalogo global completo de anuncios de retiro usa
# > `azure-release-comunication-retirements.ipynb`.


# CELL ********************

# PARAMETROS - marca esta celda como 'Parameters' en Fabric (menu ... -> Toggle parameter cell)

# --- Identidad / Key Vault (generados por 03-setup-sp-keyvault.sh; VARIAN por implementacion) ---

TENANT_ID    = '0ed1f3f1-eec8-417c-b0d5-75cf02c65a84' #'e325fd85-5068-43a4-b9d0-ff74c2e25df7'
CLIENT_ID    = '42252f37-a4b9-476d-a855-fb0711d55c6c' #'92f41483-9a85-4d0f-9b39-938bedf14ee1'
KEYVAULT_URL = 'https://kvtofabric.vault.azure.net/' #'https://kvvigenciatec01.vault.azure.net/'
SECRET_NAME  = 'svalue'



# --- Alcance y ejecucion ---
include_disabled = False        # True = incluir tambien subs deshabilitadas
subscriptions = []              # vacio = todas las Enabled; o lista explicita de subscriptionId
page_size = 1000               # $top de Resource Graph (maximo 1000)
max_concurrency = 3            # workers en paralelo (bajo: la cuota de ARG es por tenant)
api_version = '2024-04-01'     # ultima estable de Resource Graph
ingest_date = ''              # vacio = hoy (UTC), formato YYYY-MM-DD

# --- Destino OneLake ---
# El destino (ABFSS del Lakehouse) se resuelve AUTOMATICAMENTE desde el workspace.
# Staging local (disco del nodo) para escribir en streaming antes de subir a OneLake.
local_staging_root = 'Files/obsolecencia/azure/service_retirement_api'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Auth - Token de ARM via Service Principal (secreto leido de Key Vault por la workspace identity)
# TENANT_ID / CLIENT_ID / KEYVAULT_URL / SECRET_NAME se definen en la celda de PARAMETROS.
import requests
import notebookutils
from azure.identity import ClientSecretCredential

# El secreto NUNCA va en el codigo: se lee de Key Vault en runtime.
client_secret = notebookutils.credentials.getSecret(KEYVAULT_URL, SECRET_NAME)
credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, client_secret)

def get_headers():
    # azure-identity cachea y refresca el token; get_token es thread-safe.
    token = credential.get_token('https://management.azure.com/.default').token
    return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

print('Credencial lista.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Discovery de suscripciones (ARM REST) + filtro Enabled
SUBSCRIPTIONS_URL = 'https://management.azure.com/subscriptions?api-version=2020-01-01'

def list_subscriptions():
    results = []
    url = SUBSCRIPTIONS_URL
    while url:
        resp = requests.get(url, headers=get_headers())
        resp.raise_for_status()
        payload = resp.json()
        results.extend(payload.get('value', []))
        url = payload.get('nextLink')   # paginacion automatica
    return results

_all = list_subscriptions()
if subscriptions:
    _wanted = set(subscriptions)
    target = [s for s in _all if s.get('subscriptionId') in _wanted]
elif include_disabled:
    target = _all
else:
    target = [s for s in _all if str(s.get('state', '')).casefold() == 'enabled']

subscription_ids = [s['subscriptionId'] for s in target]
print(f'Suscripciones totales: {len(_all)} | objetivo: {len(subscription_ids)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Recoleccion via Resource Graph: paginacion $skipToken + throttling robusto (429)
import os, json, time
from datetime import datetime, timezone

# Query de Advisor filtrada a recomendaciones de retiro/deprecacion (por-recurso).
ADVISOR_KQL = (
    "advisorresources\n"
    "| where type =~ 'microsoft.advisor/recommendations'\n"
    "| extend p = properties\n"
    "| extend category = tostring(p.category)\n"
    "| extend impactedResourceId = tostring(p.resourceMetadata.resourceId)\n"
    "| extend impactedType = tostring(p.impactedField)\n"
    "| extend problem = tostring(p.shortDescription.problem)\n"
    "| extend solution = tostring(p.shortDescription.solution)\n"
    "| where problem has 'retir' or solution has 'retir' or problem has 'deprecat' or solution has 'deprecat'\n"
    "| project id, subscriptionId, impactedResourceId, impactedType, category, problem, solution,\n"
    "          lastUpdated = tostring(p.lastUpdated), recommendationTypeId = tostring(p.recommendationTypeId)"
)
RESOURCES_URL = f'https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version={api_version}'
MAX_RETRIES = 8

def _seconds_until_reset(resp):
    # Header de ARG: x-ms-user-quota-resets-after en formato HH:MM:SS
    raw = resp.headers.get('x-ms-user-quota-resets-after')
    if not raw:
        return None
    try:
        h, m, s = raw.split(':')
        return int(h) * 3600 + int(m) * 60 + float(s)
    except (ValueError, AttributeError):
        return None

def _post_with_retry(body):
    attempt = 0
    while True:
        resp = requests.post(RESOURCES_URL, headers=get_headers(), json=body)
        if resp.status_code != 429:
            resp.raise_for_status()
            return resp
        if attempt >= MAX_RETRIES:
            resp.raise_for_status()
        delay = _seconds_until_reset(resp) or (2 ** attempt)
        time.sleep(delay)
        attempt += 1

def iter_advisor(subscription_id):
    # Paginacion SECUENCIAL dentro de una suscripcion.
    skip_token = None
    while True:
        options = {'resultFormat': 'objectArray', '$top': page_size}
        if skip_token:
            options['$skipToken'] = skip_token
        body = {'subscriptions': [subscription_id], 'query': ADVISOR_KQL, 'options': options}
        payload = _post_with_retry(body).json()
        for row in payload.get('data', []):
            yield row
        skip_token = payload.get('$skipToken')
        if not skip_token:
            break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Landing RAW en OneLake (staging local -> notebookutils.fs.cp) + paralelismo entre suscripciones
from concurrent.futures import ThreadPoolExecutor, as_completed

_ingest = ingest_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
rel_dir = f'raw/azure_advisor_retirements/ingest_date={_ingest}'

def _to_str(v):
    return str(v) if v not in (None, '') else None

def _conf_get(key):
    # spark.conf con claves 'trident.*' -> strings limpios (evita el objeto py4j del context).
    try:
        return _to_str(spark.conf.get(key))
    except Exception:
        return None

def _ctx_get(key):
    try:
        return _to_str(notebookutils.runtime.context.get(key))  # .get() se LLAMA, no getattr
    except Exception:
        return None

def _list_lakehouse_ids(ws_id):
    for call in (
        lambda: notebookutils.lakehouse.list(ws_id),
        lambda: notebookutils.lakehouse.list(workspaceId=ws_id),
        lambda: notebookutils.lakehouse.list(),
    ):
        try:
            res = call()
        except Exception:
            continue
        ids = []
        for lh in (res or []):
            i = lh.get('id') if isinstance(lh, dict) else None
            i = _to_str(i)
            if i:
                ids.append(i)
        if ids:
            return ids
    return []

def _resolve_files_root():
    # Se obtiene AUTOMATICAMENTE desde el workspace (sin fallback a path relativo).
    ws_id = _conf_get('trident.workspace.id') or _ctx_get('currentWorkspaceId') or _ctx_get('workspaceId')
    if not ws_id:
        raise RuntimeError('No se pudo determinar el workspaceId.')

    # Lakehouse por defecto (adjunto al notebook).
    lh_id = _conf_get('trident.lakehouse.id') or _ctx_get('defaultLakehouseId')

    # Si no hay default, tomar el unico Lakehouse del workspace.
    if not lh_id:
        ids = _list_lakehouse_ids(ws_id)
        print(f'Lakehouses detectados en el workspace: {ids}')
        if len(ids) == 1:
            lh_id = ids[0]
        elif len(ids) == 0:
            raise RuntimeError(
                f'No se detectaron Lakehouses en el workspace {ws_id}. '
                'Adjunta un Lakehouse al notebook (Lakehouse explorer -> Add) y marcalo como default.'
            )
        else:
            raise RuntimeError('El workspace tiene varios Lakehouses; marca uno como default en el notebook.')

    print(f'workspaceId={ws_id} | lakehouseId={lh_id}')
    return f'abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Files'

files_root = _resolve_files_root()
dest_dir = f'{files_root}/{rel_dir}'
print(f'Destino OneLake: {dest_dir}')
try:
    notebookutils.fs.mkdirs(dest_dir)
except Exception:
    pass  # cp crea los directorios padre si no existen

# Staging local (disco del nodo): rapido y sin depender del mount blobfuse.
local_dir = f'{local_staging_root}/{rel_dir}'
os.makedirs(local_dir, exist_ok=True)

# Normalizacion a esquema estable (las filas de Advisor ya vienen proyectadas y planas).
def _to_row(r, subscription_id):
    return {
        'id': r.get('id', ''),
        'subscription_id': r.get('subscriptionId') or subscription_id,
        'impacted_resource_id': r.get('impactedResourceId'),
        'impacted_type': r.get('impactedType'),
        'category': r.get('category'),
        'problem': r.get('problem'),
        'solution': r.get('solution'),
        'last_updated': r.get('lastUpdated'),
        'recommendation_type_id': r.get('recommendationTypeId'),
    }

def _exists(path):
    try:
        return notebookutils.fs.exists(path)
    except Exception:
        return False

def _cp_overwrite(src, dst):
    # notebookutils.fs.cp no sobrescribe: si el destino existe, lo borra y reintenta.
    try:
        notebookutils.fs.cp(src, dst)
    except Exception:
        try:
            notebookutils.fs.rm(dst, True)
        except Exception:
            pass
        notebookutils.fs.cp(src, dst)

def collect_subscription(subscription_id):
    # Salida como .json (arreglo JSON valido) para que OneLake lo previsualice.
    dest_final = f'{dest_dir}/subscription={subscription_id}.json'
    if _exists(dest_final):
        return ('skipped', subscription_id, 0)   # checkpoint: ya en OneLake
    local_final = f'{local_dir}/subscription={subscription_id}.json'
    count = 0
    try:
        with open(local_final, 'w', encoding='utf-8') as f:
            f.write('[')
            first = True
            for raw in iter_advisor(subscription_id):       # streaming, sin acumular en memoria
                row = _to_row(raw, subscription_id)
                f.write('\n' if first else ',\n')
                f.write('  ' + json.dumps(row, ensure_ascii=False))
                first = False
                count += 1
            f.write('\n]\n' if not first else ']\n')
        # Subida a OneLake (whole-file, idempotente); el archivo final existe = 'done'.
        _cp_overwrite(f'file://{local_final}', dest_final)
        os.remove(local_final)                  # limpia staging local
        return ('done', subscription_id, count)
    except Exception as e:
        if os.path.exists(local_final):
            os.remove(local_final)              # descarta parcial -> se rehace el proximo run
        return ('error', subscription_id, repr(e))

results = []
with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
    futures = {pool.submit(collect_subscription, sid): sid for sid in subscription_ids}
    for fut in as_completed(futures):
        status, sid, info = fut.result()
        results.append((status, sid, info))
        print(f'[{status}] {sid} -> {info}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificacion
import pandas as pd

n_done = sum(1 for r in results if r[0] == 'done')
n_skip = sum(1 for r in results if r[0] == 'skipped')
n_err  = sum(1 for r in results if r[0] == 'error')
total_recs = sum(int(r[2]) for r in results if r[0] == 'done')

print(f'Suscripciones -> done={n_done} skipped={n_skip} error={n_err}')
print(f'Recomendaciones de retiro nuevas: {total_recs}')
print(f'Landing: {dest_dir}')

summary = pd.DataFrame(results, columns=['status', 'subscription_id', 'detail'])
display(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Previsualizar las recomendaciones de retiro aterrizadas (lee los .json de OneLake)
# multiline=true: cada archivo es un arreglo JSON. Esquema plano.



from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id", StringType()),
    StructField("subscription_id", StringType()),
    StructField("impacted_resource_id", StringType()),
    StructField("impacted_type", StringType()),
    StructField("category", StringType()),
    StructField("problem", StringType()),
    StructField("solution", StringType()),
    StructField("last_updated", StringType()),
    StructField("recommendation_type_id", StringType()),
])

df_adv = spark.read.option('multiline', 'true').schema(schema).json(dest_dir)


cols = df_adv.columns  # vacio si Advisor no devolvio ninguna recomendacion (arreglos [] -> Relation [])

if not cols:
    print(f'Sin recomendaciones de retiro de Advisor en {rel_dir}.')
    print('Advisor solo emite recomendaciones de retiro por-recurso para un subconjunto de servicios;')
    print('para el catalogo global usa azure-release-comunication-retirements.ipynb.')
else:
    print(f'Total recomendaciones de retiro en {rel_dir}: {df_adv.count()}')
    _pref = ['subscription_id', 'impacted_type', 'impacted_resource_id', 'category', 'problem']
    _sel = [c for c in _pref if c in cols] or cols
    df_adv.select(*_sel).show(20, truncate=False)
    display(df_adv.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
