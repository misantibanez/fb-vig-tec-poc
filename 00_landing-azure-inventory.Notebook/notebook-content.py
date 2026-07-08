# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Inventario de recursos Azure (Fabric)
# 
# Extiende el PoC de discovery a un **inventario completo de recursos** vía Azure Resource Graph,
# con **landing raw en OneLake** (JSONL). Ver `PLAN.md`.
# 
# **Prerequisitos** (scripts .sh ya ejecutados): `01-provisioning-fabric.sh`,
# `02-setup-workspace-identity.sh`, `03-setup-sp-keyvault.sh`.
# 
# **Diseno:** unidad = 1 suscripcion (paralelismo entre subs, paginacion `$skipToken` secuencial
# dentro de cada una), escritura streaming `.tmp` -> rename atomico como checkpoint, throttling
# robusto con `x-ms-user-quota-resets-after`. Requiere un **Lakehouse adjunto** al notebook.

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
local_staging_root = '/tmp/azure_inventory'

# Forzar destino compatible con notebook 01 (brz_vigencia).
target_lakehouse_name = 'brz_vigencia'

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

# Fuente de verdad de la query (identica a ../inventory resource_collection.py)
RESOURCES_KQL = (
    'Resources\n'
    '| project id, name, type, resourceGroup, location, subscriptionId, kind, sku, tags, properties'
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

def iter_resources(subscription_id):
    # Paginacion SECUENCIAL dentro de una suscripcion.
    skip_token = None
    while True:
        options = {'resultFormat': 'objectArray', '$top': page_size}
        if skip_token:
            options['$skipToken'] = skip_token
        body = {'subscriptions': [subscription_id], 'query': RESOURCES_KQL, 'options': options}
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

_ingest = ingest_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
rel_dir = f'vigencia/inventory/azure/ingest_date={_ingest}/run_id={run_id}'

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

def _list_lakehouses(ws_id):
    for call in (
        lambda: notebookutils.lakehouse.list(ws_id),
        lambda: notebookutils.lakehouse.list(workspaceId=ws_id),
        lambda: notebookutils.lakehouse.list(),
    ):
        try:
            res = call()
        except Exception:
            continue
        out = []
        for lh in (res or []):
            if not isinstance(lh, dict):
                continue
            _id = _to_str(lh.get('id'))
            _name = _to_str(lh.get('displayName') or lh.get('name'))
            if _id:
                out.append({'id': _id, 'displayName': _name})
        if out:
            return out
    return []

def _resolve_files_root():
    ws_id = _conf_get('trident.workspace.id') or _ctx_get('currentWorkspaceId') or _ctx_get('workspaceId')
    if not ws_id:
        raise RuntimeError('No se pudo determinar el workspaceId.')

    # Lakehouse por defecto adjunto al notebook (si existe).
    default_lh_id = _conf_get('trident.lakehouse.id') or _ctx_get('defaultLakehouseId')

    # Descubrir lakehouses para poder fijar el destino esperado por notebook 01.
    lakehouses = _list_lakehouses(ws_id)
    target = [lh for lh in lakehouses if (lh.get('displayName') or '').lower() == target_lakehouse_name.lower()]

    if target:
        lh_id = target[0]['id']
        print(f"Usando lakehouse por nombre objetivo: {target_lakehouse_name} ({lh_id})")
    elif default_lh_id:
        lh_id = default_lh_id
        print(f"Usando lakehouse default adjunto: {lh_id}")
    elif len(lakehouses) == 1:
        lh_id = lakehouses[0]['id']
        print(f"Usando unico lakehouse detectado: {lh_id}")
    elif len(lakehouses) == 0:
        raise RuntimeError(
            f'No se detectaron Lakehouses en el workspace {ws_id}. '
            'Adjunta un Lakehouse al notebook (Lakehouse explorer -> Add) y marcalo como default.'
        )
    else:
        ids = [f"{lh.get('displayName')}:{lh.get('id')}" for lh in lakehouses]
        raise RuntimeError(
            'El workspace tiene varios Lakehouses y no se encontro el objetivo '
            f"'{target_lakehouse_name}'. Configura target_lakehouse_name o marca uno como default. "
            f"Detectados: {ids}"
        )

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

# Row canonico para Bronze (tags/properties/sku como objetos JSON nativos).
def _to_row(r, subscription_id):
    return {
        'id': r.get('id', ''),
        'name': r.get('name', ''),
        'type': r.get('type', ''),
        'subscription_id': r.get('subscriptionId') or subscription_id,
        'resource_group': r.get('resourceGroup'),
        'location': r.get('location'),
        'kind': r.get('kind'),
        'sku': r.get('sku'),
        'tags': r.get('tags'),
        'properties': r.get('properties'),
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

dest_file = f'{dest_dir}/resources.json'
local_file = f'{local_dir}/resources.json'

results = []

def collect_to_single_file(subscription_ids):
    total = 0
    with open(local_file, 'w', encoding='utf-8') as f:
        f.write('[')
        first = True
        for sid in subscription_ids:
            count = 0
            try:
                for raw in iter_resources(sid):
                    row = _to_row(raw, sid)
                    f.write('\n' if first else ',\n')
                    f.write('  ' + json.dumps(row, ensure_ascii=False))
                    first = False
                    count += 1
                    total += 1
                results.append(('done', sid, count))
                print(f'[done] {sid} -> {count}')
            except Exception as e:
                results.append(('error', sid, repr(e)))
                print(f'[error] {sid} -> {e!r}')
        f.write('\n]\n' if not first else ']\n')

    _cp_overwrite(f'file://{local_file}', dest_file)
    os.remove(local_file)
    return total


total_resources = collect_to_single_file(subscription_ids)

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
total_recursos = total_resources

print(f'Suscripciones -> done={n_done} skipped={n_skip} error={n_err}')
print(f'Recursos nuevos inventariados: {total_recursos}')
print(f'Landing snapshot file: {dest_file}')

summary = pd.DataFrame(results, columns=['status', 'subscription_id', 'detail'])
display(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Previsualizar los recursos aterrizados (lee resources.json en OneLake)
# Nota: algunos providers traen campos con diferencias de mayus/minus dentro de `properties`.
# Forzamos caseSensitive para evitar colisiones de schema (ej. creationDate vs creationdate).
spark.conf.set('spark.sql.caseSensitive', 'true')

df_recursos = spark.read.option('multiline', 'true').json(dest_file)
print(f'Total de recursos en {dest_file}: {df_recursos.count()}')
df_recursos.select('subscription_id', 'resource_group', 'type', 'name', 'location', 'kind').show(20, truncate=False)
display(df_recursos.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
