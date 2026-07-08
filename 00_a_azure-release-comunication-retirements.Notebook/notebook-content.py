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

# # Retiros / obsolescencia de Azure - Release Communications feed (global)
# 
# Fuente unica: el feed publico **Azure Release Communications**
# (`https://www.microsoft.com/releasecommunications/api/v2/azure`).
# 
# Responde: **que se retira y cuando**, a nivel de producto Azure (catalogo global).
# No cruza contra tu inventario ni contra Advisor/Service Health: es el catalogo
# completo de anuncios etiquetados como `Retirements`.
# 
# **Sin auth:** el feed es publico; no requiere Service Principal, Key Vault ni permisos.
# 
# **Ejecucion autonoma:** corre **Run all** (o programa el notebook en un pipeline de Fabric).
# Todo el trabajo lo hace un unico `main()`.
# 
# **Idempotente:** cada `ingest_date` es un `.json` en OneLake; si ya existe, se salta
# (checkpoint). Usa `force_refresh=True` para reescribir.
# 
# **Prerequisito:** un **Lakehouse adjunto** al notebook (marcado como default).

# CELL ********************

# PARAMETROS - marca esta celda como 'Parameters' en Fabric (menu ... -> Toggle parameter cell)

# --- Fuente: Azure Release Communications feed (publico, sin auth) ---
api_base  = 'https://www.microsoft.com/releasecommunications/api/v2/azure'
page_size = 100          # el feed pagina de 100 en 100 con $skip
max_items = 3000         # tope de items a recorrer

# --- Filtro ---
pattern       = ''       # regex case-insensitive en titulo/descripcion. '' = todos los retirements
upcoming_only = False    # True = solo retiros con fecha hoy o futura (o sin fecha)
year          = None     # int: solo retiros cuya fecha de retiro cae en este anio (None = sin filtro)
since         = None     # int: solo retiros de este anio en adelante (None = sin filtro)

# --- Ejecucion / destino ---
ingest_date        = ''                          # vacio = hoy (UTC), formato YYYY-MM-DD
force_refresh      = False                        # True = reescribe aunque ya exista en OneLake
local_staging_root = '/tmp/azure_release_retirements'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================================
# MOTOR (solo definiciones). No ejecuta trabajo: define funciones, constantes y main().
# Fuente unica: Azure Release Communications feed (global, sin auth). Idempotente por diseno.
# ================================================================================
import os, re, json, urllib.request
from datetime import date, datetime, timezone

import notebookutils

RETIREMENT_TAG = 'Retirements'
DATASET = 'release_communications'

_MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
}
_MONTHS_RE = '|'.join(_MONTHS)
# El feed usa dos ordenes de fecha:
#   'DIA Mes ANIO' -> '1 October 2026', '29 February 2024'  (el mas comun)
#   'Mes DIA, ANIO' -> 'October 1, 2026', 'March 31, 2028'
_DMY_RE = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(' + _MONTHS_RE + r')\s+(\d{4})\b', re.IGNORECASE)
_MDY_RE = re.compile(r'\b(' + _MONTHS_RE + r')\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b', re.IGNORECASE)

def extract_retire_date(text):
    'Extrae la primera fecha de retiro. Soporta dia-mes-anio y mes-dia-anio.'
    text = text or ''
    best = None  # (posicion_en_texto, date) -> nos quedamos con la mas temprana
    for rx, dmy in ((_DMY_RE, True), (_MDY_RE, False)):
        m = rx.search(text)
        if not m:
            continue
        if dmy:
            day, month, year = int(m.group(1)), _MONTHS[m.group(2).lower()], int(m.group(3))
        else:
            month, day, year = _MONTHS[m.group(1).lower()], int(m.group(2)), int(m.group(3))
        try:
            d = date(year, month, day)
        except ValueError:
            continue
        if best is None or m.start() < best[0]:
            best = (m.start(), d)
    return best[1] if best else None

def parse_page(raw):
    'Decodifica una pagina del feed tolerando caracteres de control.'
    data = json.loads(raw.decode('utf-8', 'replace'), strict=False)
    return data.get('value', [])

def fetch_all():
    'Recorre todas las paginas del feed y devuelve los items crudos.'
    if not api_base.startswith('https://'):
        raise ValueError(f'Solo se permiten URLs https, recibido: {api_base!r}')
    items, skip = [], 0
    while skip < max_items:
        with urllib.request.urlopen(f'{api_base}?$skip={skip}') as resp:
            page = parse_page(resp.read())
        if not page:
            break
        items.extend(page)
        skip += page_size
    return items

def is_retirement(item):
    return RETIREMENT_TAG in (item.get('tags') or [])

def matches(item, rx):
    text = f"{item.get('title', '')} {item.get('description') or ''}"
    return rx.search(text) is not None

def normalize(item):
    title = (item.get('title') or '').strip()
    text = f"{title} {item.get('description') or ''}"
    rd = extract_retire_date(text)
    return {
        'created': (item.get('created') or '')[:10],
        'title': title,
        'status': item.get('status'),
        'retire_date': rd.isoformat() if rd else None,
        'retire_year': rd.year if rd else None,
    }

def filter_retirements(items):
    'Retirements que coinciden con el patron, sin duplicados.'
    rx = re.compile(pattern or '', re.IGNORECASE)
    seen, out = set(), []
    for item in items:
        if is_retirement(item) and matches(item, rx):
            row = normalize(item)
            key = (row['created'], row['title'])
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
    out.sort(key=lambda r: (r['retire_date'] or '9999-12-31', r['title']))
    return out

def apply_filters(rows):
    'Aplica los filtros opcionales upcoming/year/since.'
    today = datetime.now(timezone.utc).date().isoformat()
    if upcoming_only:
        rows = [r for r in rows if r['retire_date'] is None or r['retire_date'] >= today]
    if year is not None:
        rows = [r for r in rows if r['retire_year'] == year]
    if since is not None:
        rows = [r for r in rows if r['retire_year'] is not None and r['retire_year'] >= since]
    return rows

# --- Landing OneLake (resolucion automatica del Lakehouse) ---
def _to_str(v):
    return str(v) if v not in (None, '') else None

def _conf_get(key):
    try:
        return _to_str(spark.conf.get(key))
    except Exception:
        return None

def _ctx_get(key):
    try:
        return _to_str(notebookutils.runtime.context.get(key))
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
        ids = [_to_str(lh.get('id')) for lh in (res or []) if isinstance(lh, dict) and lh.get('id')]
        ids = [i for i in ids if i]
        if ids:
            return ids
    return []

def resolve_files_root():
    ws_id = _conf_get('trident.workspace.id') or _ctx_get('currentWorkspaceId') or _ctx_get('workspaceId')
    if not ws_id:
        raise RuntimeError('No se pudo determinar el workspaceId.')
    lh_id = _conf_get('trident.lakehouse.id') or _ctx_get('defaultLakehouseId')
    if not lh_id:
        ids = _list_lakehouse_ids(ws_id)
        if len(ids) == 1:
            lh_id = ids[0]
        elif not ids:
            raise RuntimeError('No hay Lakehouse adjunto. Adjunta uno al notebook y marcalo como default.')
        else:
            raise RuntimeError('Varios Lakehouses; marca uno como default en el notebook.')
    print(f'workspaceId={ws_id} | lakehouseId={lh_id}')
    return f'abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Files'

def _exists(path):
    try:
        return notebookutils.fs.exists(path)
    except Exception:
        return False

def _cp_overwrite(src, dst):
    try:
        notebookutils.fs.cp(src, dst)
    except Exception:
        try:
            notebookutils.fs.rm(dst, True)
        except Exception:
            pass
        notebookutils.fs.cp(src, dst)

def ingest_date_value():
    return ingest_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')

def dataset_dir(files_root, ing=None):
    return f'{files_root}/obsolecencia/azure/{DATASET}/ingest_date={ing or ingest_date_value()}'

# --- Orquestador unico: autonomo e idempotente ---
def main():
    files_root = resolve_files_root()
    ing = ingest_date_value()
    dest_dir = dataset_dir(files_root, ing)
    dest_final = f'{dest_dir}/retirements.json'

    if _exists(dest_final) and not force_refresh:
        print(f'[skipped] ya existe {dest_final} (force_refresh=True para reescribir)')
        n_new = None
    else:
        rows = apply_filters(filter_retirements(fetch_all()))
        local_dir = f'{local_staging_root}/obsolecencia/azure/{DATASET}/ingest_date={ing}'
        os.makedirs(local_dir, exist_ok=True)
        local_final = f'{local_dir}/retirements.json'
        with open(local_final, 'w', encoding='utf-8') as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        try:
            notebookutils.fs.mkdirs(dest_dir)
        except Exception:
            pass
        _cp_overwrite(f'file://{local_final}', dest_final)
        os.remove(local_final)
        n_new = len(rows)
        print(f'[done] {n_new} retirements -> {dest_final}')

    df = spark.read.option('multiline', 'true').json(dest_dir)
    total = df.count()
    tag = f' (nuevos: {n_new})' if n_new is not None else ' (checkpoint)'
    print(f'Total retirements en {ing}: {total}{tag}')
    return {'files_root': files_root, 'ingest_date': ing, 'dest_dir': dest_dir,
            'count': total, 'df': df}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================================
# EJECUCION. Un solo llamado descarga el feed, filtra y materializa en OneLake.
# Re-ejecutar es seguro: lo ya materializado se salta (checkpoint). Autonomo para Run all
# o para un pipeline programado de Fabric.
# ================================================================================
resultado = main()
display(resultado['df'].orderBy('retire_date'))

# Para un pipeline de Fabric puedes devolver el conteo al orquestador:
# notebookutils.notebook.exit(str(resultado['count']))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Previsualizacion: lee de OneLake (fuente de verdad). No depende de estado en memoria:
# recalcula las rutas con las mismas funciones del motor, asi es re-ejecutable de forma aislada.
files_root = resolve_files_root()
df = spark.read.option('multiline', 'true').json(dataset_dir(files_root))
print(f'Retirements: {df.count()}')
df.select('created', 'retire_date', 'retire_year', 'status', 'title').orderBy('retire_date').show(50, truncate=False)
display(df.limit(200))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
