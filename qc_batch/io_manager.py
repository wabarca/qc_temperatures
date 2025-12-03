#!/usr/bin/env python3
"""
io_manager.py

Funciones para:
 - detectar archivo candidato (qc -> tmp -> org)
 - leer series CSV normalizando -99.0/-99.9 -> -99
 - escribir _tmp.csv y _qc.csv en folder_out
 - utilidades de nombres y listados

Uso:
  from io_manager import find_candidate_file, read_series, write_tmp, write_qc
"""

from pathlib import Path
import pandas as pd
import re
from typing import Optional, Dict, Any

# nombre esperado: var_periodo_estacion_org.csv
FNAME_RE = re.compile(
    r"^(?P<var>[^_]+)_(?P<periodo>[^_]+)_(?P<estacion>[^_]+?)(?:_(?P<suffix>org|tmp|qc))?\.csv$",
    re.IGNORECASE,
)


def parse_filename(fname: str) -> Optional[Dict[str, str]]:
    """Parsea nombre de archivo y devuelve dict con var, periodo, estacion, suffix (org/tmp/qc o None)"""
    m = FNAME_RE.match(Path(fname).name)
    if not m:
        return None
    return {k: (v.lower() if v else None) for k, v in m.groupdict().items()}


def build_filename(var: str, periodo: str, estacion: str, suffix: str):
    """
    Construye nombre: {var}_{periodo}_{estacion}_{suffix}.csv
    suffix ∈ {'org','tmp','QC','qc'}
    """
    var = var.lower()
    suffix = suffix.lower()
    # usar QC mayúscula en nombre final para compatibilidad con tu flujo original
    if suffix == "qc":
        suf = "QC"
    else:
        suf = suffix
    return f"{var}_{periodo}_{estacion}_{suf}.csv"


def _safe_read_csv(path: Path) -> pd.DataFrame:
    """Lee CSV intentando detectar delimitador; devuelve DataFrame"""
    try:
        df = pd.read_csv(path, sep=None, engine="python")
    except Exception:
        # fallback: comma
        df = pd.read_csv(path)
    return df


def read_series(path: str) -> pd.DataFrame:
    """
    Lee un archivo CSV de dos columnas:
       FECHA, <NOMBRE_ESTACION>
    y lo convierte en un formato estándar:
       fecha, valor
    """
    import pandas as pd

    df = pd.read_csv(path, sep=None, engine="python")
    df.columns = [c.strip().lower() for c in df.columns]

    # Renombrar primera columna → fecha
    df = df.rename(columns={df.columns[0]: "fecha"})

    # Renombrar segunda columna → valor
    df = df.rename(columns={df.columns[1]: "valor"})

    # Convertir fecha
    df["fecha"] = pd.to_datetime(
        df["fecha"].astype(str), format="%Y%m%d", errors="coerce"
    )

    # Normalizar valor
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(-99)
    df["valor"] = df["valor"].replace([-99.0, -99.9, -99.00], -99)

    return df


def _ensure_outdir(folder_out: str):
    Path(folder_out).mkdir(parents=True, exist_ok=True)


def write_tmp(
    df: pd.DataFrame, folder_out: str, var: str, periodo: str, estacion: str
) -> str:
    """
    Guarda DataFrame como var_periodo_estacion_tmp.csv en folder_out.
    El dataframe debe tener columnas ['fecha','valor'] donde 'fecha' es datetime.
    Devuelve la ruta escrita.
    """
    _ensure_outdir(folder_out)
    fname = build_filename(var, periodo, estacion, "tmp")
    p = Path(folder_out) / fname
    df_out = df.copy()
    # formatear fecha a YYYYMMDD
    df_out["fecha"] = df_out["fecha"].dt.strftime("%Y%m%d")
    df_out.to_csv(p, index=False)
    return str(p)


def write_qc(
    df: pd.DataFrame, folder_out: str, var: str, periodo: str, estacion: str
) -> str:
    """
    Guarda DataFrame como var_periodo_estacion_qc.csv en folder_out.
    Devuelve la ruta escrita.
    """
    _ensure_outdir(folder_out)
    fname = build_filename(var, periodo, estacion, "qc")
    p = Path(folder_out) / fname
    df_out = df.copy()
    df_out["fecha"] = df_out["fecha"].dt.strftime("%Y%m%d")
    df_out.to_csv(p, index=False)
    return str(p)


def find_candidate_file(
    folder_in: str, folder_out: str, var: str, periodo: str, estacion: str
) -> Dict[str, Any]:
    """
    Prioridad de retorno:
      1) *_tmp.csv  (intermedio en folder_out)
      2) *_QC.csv   (final en folder_out)
      3) *_org.csv  (original en folder_in)
    Retorna dict {"status": "tmp"|"qc"|"org"|None, "path": path_or_None, "base_name": base_name_str}
    """
    folder_in = Path(folder_in)
    folder_out = Path(folder_out)

    candidates = []

    # construir nombres esperados
    fname_tmp = build_filename(var, periodo, estacion, "tmp")
    fname_qc = build_filename(var, periodo, estacion, "qc")
    fname_org = build_filename(var, periodo, estacion, "org")

    p_tmp = folder_out / fname_tmp
    if p_tmp.exists():
        return {"status": "tmp", "path": str(p_tmp), "base_name": fname_tmp}

    p_qc = folder_out / fname_qc
    if p_qc.exists():
        return {"status": "qc", "path": str(p_qc), "base_name": fname_qc}

    # buscar org en folder_in (aceptar también archivos sin sufijo org, por compatibilidad)
    p_org = folder_in / fname_org
    if p_org.exists():
        return {"status": "org", "path": str(p_org), "base_name": fname_org}

    # fallback: intentar sin sufijo org (var_periodo_estacion.csv)
    fallback = folder_in / f"{var}_{periodo}_{estacion}.csv"
    if fallback.exists():
        return {"status": "org", "path": str(fallback), "base_name": fallback.name}

    return {"status": None, "path": None, "base_name": None}


def list_candidates(
    folder_in: str, folder_out: str, prefixes: Optional[list] = None
) -> list:
    """
    Lista archivos en folder_in que cumplan el patrón var_periodo_estacion_org.csv
    Retorna lista de dicts parseados con parse_filename
    """
    folder_in = Path(folder_in)
    files = []
    for f in sorted(folder_in.glob("*.csv")):
        parsed = parse_filename(f.name)
        if not parsed:
            continue
        # solo incluir archivos con sufijo org o sin sufijo (para compatibilidad)
        # consideramos var en allowed si se pasa prefixes
        if prefixes and parsed["var"] not in prefixes:
            continue
        files.append({"path": str(f), **parsed})
    return files


def normalize_var_name(var: str) -> str:
    v = (var or "").strip().lower()
    if v == "ts":
        return "tmean"
    return v
