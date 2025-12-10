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
    return f"{var}_{periodo}_{estacion.upper()}_{suf}.csv"


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
    Lee un archivo CSV con formato:
       FECHA,<ID_ESTACION>
    y lo normaliza al formato interno:
       fecha (datetime), valor (float)
    """
    df = _safe_read_csv(path)

    cols = list(df.columns)

    if len(cols) < 2:
        raise ValueError(f"Archivo inválido: {path}")

    # columna de estación = segunda columna
    col_id = cols[1]

    df = df.rename(columns={"FECHA": "fecha", col_id: "valor"})

    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d", errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # eliminar filas totalmente inválidas
    df = df.dropna(subset=["fecha"])

    return df[["fecha", "valor"]]


def _ensure_outdir(folder_out: str):
    Path(folder_out).mkdir(parents=True, exist_ok=True)


# en io_manager.py — reemplazar write_tmp por esta versión
def write_tmp(
    df: pd.DataFrame, folder_out: str, var: str, periodo: str, estacion: str
) -> str:
    _ensure_outdir(folder_out)
    estacion_upper = estacion.upper()
    fname = build_filename(var, periodo, estacion_upper, "tmp")
    p = Path(folder_out) / fname

    df_out = df.copy()
    # FECHA como YYYYMMDD
    df_out["FECHA"] = pd.to_datetime(df_out["fecha"]).dt.strftime("%Y%m%d")
    # colocar valores en columna con nombre de estación (numéricos)
    df_out[estacion_upper] = df_out["valor"]
    # mantener solo FECHA y <ID_ESTACION>
    df_out = df_out[["FECHA", estacion_upper]]

    p.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(p, index=False)
    return str(p)


def write_qc(
    df: pd.DataFrame, folder_out: str, var: str, periodo: str, estacion: str
) -> str:
    """
    Guarda DataFrame como var_periodo_estacion_qc.csv en formato:
    FECHA,<ID_ESTACION>
    """
    _ensure_outdir(folder_out)

    estacion_upper = estacion.upper()
    col_id = estacion_upper

    fname = build_filename(var, periodo, estacion_upper, "qc")
    p = Path(folder_out) / fname

    df_out = df.copy()

    # FECHA → YYYYMMDD
    df_out["FECHA"] = pd.to_datetime(df_out["fecha"]).dt.strftime("%Y%m%d")

    # valor → <ID_ESTACION>
    df_out[col_id] = df_out["valor"]

    # mantener solo FECHA y ID
    df_out = df_out[["FECHA", col_id]]

    df_out.to_csv(p, index=False)
    return str(p)


def find_candidate_file(
    folder_in: str, folder_out: str, var: str, periodo: str, estacion: str
) -> Dict[str, Any]:
    """
    Prioridad de retorno:
      1) *_qc.csv   (final en folder_out)
      2) *_tmp.csv  (intermedio en folder_out)
      3) *_org.csv  (original en folder_in)
      4) fallback sin sufijo org (var_periodo_estacion.csv)
    """
    folder_in = Path(folder_in)
    folder_out = Path(folder_out)

    # Construir nombres esperados
    fname_qc = build_filename(var, periodo, estacion, "qc")
    fname_tmp = build_filename(var, periodo, estacion, "tmp")
    fname_org = build_filename(var, periodo, estacion, "org")

    # 1) Buscar QC
    p_qc = folder_out / fname_qc
    if p_qc.exists():
        return {"status": "qc", "path": str(p_qc), "base_name": fname_qc}

    # 2) Buscar TMP
    p_tmp = folder_out / fname_tmp
    if p_tmp.exists():
        return {"status": "tmp", "path": str(p_tmp), "base_name": fname_tmp}

    # 3) Buscar ORG (incluye sufijo _org.csv)
    p_org = folder_in / fname_org
    if p_org.exists():
        return {"status": "org", "path": str(p_org), "base_name": fname_org}

    # 4) Fallback (var_periodo_estacion.csv sin sufijo)
    fallback = folder_in / f"{var}_{periodo}_{estacion}.csv"
    if fallback.exists():
        return {"status": "org", "path": str(fallback), "base_name": fallback.name}

    # No se encontró nada
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
