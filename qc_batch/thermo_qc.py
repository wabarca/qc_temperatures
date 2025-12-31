#!/usr/bin/env python3
"""
thermo_qc.py

Módulo de control termodinámico:
 - carga el triplete tmin/tmean/tmax (prioridad tmp -> org)
 - detecta inconsistencias termodinámicas
 - aplica correcciones interactivas (i, t, x, e, s, m, r)
 - escribe *_tmp.csv con los tres series actualizadas
 - registra cambios en changes_applied.json (solo como bitácora)
"""

from pathlib import Path
from typing import Dict, Optional, List, Any
import pandas as pd
import json
from datetime import datetime, timezone
from qc_batch.io_manager import extract_period_from_filename

# Intentamos usar io_manager existente
try:
    from qc_batch.io_manager import (
        find_candidate_file,
        read_series,
        build_filename,
    )
except Exception:
    # fallback: try to import from local path if not packaged
    from .io_manager import find_candidate_file, read_series, build_filename

CHANGES_FNAME = "changes_applied.json"
COMPLETED_FNAME = "completed_series.json"


def find_candidate_any_period(folder_out, var, estacion):
    estacion = estacion.upper()
    folder_out = Path(folder_out)

    # Prioridad: QC → TMP
    patrones = [
        f"{var}_*_{estacion}_QC.csv",
        f"{var}_*_{estacion}_qc.csv",
        f"{var}_*_{estacion}_tmp.csv",
        f"{var}_*_{estacion}_TMP.csv",
    ]

    for patron in patrones:
        matches = sorted(folder_out.glob(patron))
        if matches:
            return str(matches[0])

    return None


def _path_changes(folder_out: str) -> Path:
    p = Path(folder_out) / CHANGES_FNAME
    return p


def _load_changes(folder_out: str) -> Dict[str, Any]:
    p = _path_changes(folder_out)
    if not p.exists():
        return {"swaps": [], "single_changes": []}
    try:
        with p.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {"swaps": [], "single_changes": []}


def _save_changes(folder_out: str, changes: Dict[str, Any]):
    p = _path_changes(folder_out)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        json.dump(changes, fh, indent=2, ensure_ascii=False)


def load_triplet(
    folder_in: str,
    folder_out: str,
    periodo: str,
    estacion: str,
    start_from: str = "auto",
    force_org: bool = False,
):
    """
    Carga tmin, tmean, tmax y pr para la estación indicada.

    Prioridad (modo AUTO):
        1) *_QC.csv    (folder_out, cualquier periodo)
        2) *_tmp.csv   (folder_out, cualquier periodo)
        3) *_org.csv   (folder_in, periodo exacto)
        4) fallback flexible (solo ORG)
    """

    estacion = estacion.upper()

    res = {}
    paths_trip = {}

    variables = ["tmin", "tmean", "tmax", "pr"]

    for var in variables:
        # PR siempre se carga desde ORG
        if var == "pr":
            # PR se usa solo como referencia: cargar ORG ignorando periodo
            pattern = f"pr_*_{estacion}_org.csv"
            matches = sorted(Path(folder_in).glob(pattern))

            if matches:
                ruta_usada = str(matches[0])
                df_loaded = read_series(ruta_usada)
            else:
                ruta_usada = "N/D"
                df_loaded = None

            res[var] = df_loaded
            paths_trip[var] = ruta_usada
            continue

        # ==================================================
        # MODO FORZADO ORG (desde cero REAL)
        # ==================================================
        if force_org:
            path = Path(folder_in) / build_filename(var, periodo, estacion, "org")
            if path.exists():
                ruta_usada = str(path)
                df_loaded = read_series(ruta_usada)

        # ==================================================
        # MODO AUTO (incremental por estación)
        # ==================================================
        else:

            # ==================================================
            # MODO QC (revisión parcial)
            # ==================================================
            if start_from == "qc":
                path_any = find_candidate_any_period(folder_out, var, estacion)

                if path_any and path_any.lower().endswith("_qc.csv"):
                    ruta_usada = path_any
                    df_loaded = read_series(ruta_usada)
                else:
                    raise FileNotFoundError(
                        f"[QC] No se encontró archivo QC para {var.upper()} en estación {estacion}"
                    )

            # ==================================================
            # MODO AUTO (incremental)
            # ==================================================
            else:
                path_any = find_candidate_any_period(folder_out, var, estacion)

                if path_any:
                    ruta_usada = path_any
                    df_loaded = read_series(ruta_usada)

                # fallback ORG exacto
                if df_loaded is None:
                    cand = find_candidate_file(
                        folder_in, folder_out, var, periodo, estacion
                    )
                    if cand.get("path"):
                        ruta_usada = cand["path"]
                        df_loaded = read_series(ruta_usada)

        # ==================================================
        # Fallback flexible SOLO para ORG
        # ==================================================
        if df_loaded is None and not force_org and start_from != "qc":
            pattern = f"{var}_*_{estacion}_org.csv"
            matches = sorted(Path(folder_in).glob(pattern))
            if matches:
                ruta_usada = str(matches[0])
                df_loaded = read_series(ruta_usada)
                print(
                    f"[FALLBACK] Usando archivo {matches[0].name} para {var.upper()} en estación {estacion}"
                )

        res[var] = df_loaded
        paths_trip[var] = ruta_usada

    return res, paths_trip


def detect_thermal_inconsistencies(
    df_tmin: Optional[pd.DataFrame],
    df_tmean: Optional[pd.DataFrame],
    df_tmax: Optional[pd.DataFrame],
) -> List[Dict[str, Any]]:
    """
    Detecta filas donde no se cumpla tmin < tmean < tmax.
    Solo compara fechas donde los 3 valores no sean -99.
    Devuelve lista de dicts:
      {'fecha': Timestamp, 'tmin': float, 'tmean': float, 'tmax': float, 'tipo': str}
    Tipos: 'tmax<tmin','tmean>tmax','tmean<tmin','tmin==tmax','tmean==tmax','tmean==tmin','indefinido'
    """
    # Preparar merge
    dfs = []
    if df_tmin is not None:
        d = df_tmin.rename(columns={"valor": "tmin"})
        dfs.append(d)
    if df_tmean is not None:
        d = df_tmean.rename(columns={"valor": "tmean"})
        dfs.append(d)
    if df_tmax is not None:
        d = df_tmax.rename(columns={"valor": "tmax"})
        dfs.append(d)

    if not dfs:
        return []

    from functools import reduce

    df_merged = reduce(lambda a, b: pd.merge(a, b, on="fecha", how="outer"), dfs)
    df_merged = df_merged.sort_values("fecha").reset_index(drop=True)

    inconsistencias = []
    for _, row in df_merged.iterrows():
        tmin = row.get("tmin", -99)
        tmean = row.get("tmean", -99)
        tmax = row.get("tmax", -99)
        # skip if any missing marker
        # saltar solo si TODOS están vacíos o -99
        if all(pd.isna(x) or x == -99 for x in (tmin, tmean, tmax)):
            continue

        tipo = None
        try:
            # ============================
            # Validación por pares válidos
            # ============================

            valores = {
                "tmin": tmin,
                "tmean": tmean,
                "tmax": tmax,
            }

            # Identificar cuáles son válidos (≠ -99)
            validos = {k: (v != -99 and not pd.isna(v)) for k, v in valores.items()}
            num_validos = sum(validos.values())

            # Si no hay valores útiles → omitir
            if num_validos == 0:
                continue

            tipo = None

            # --- Comparaciones iguales ---
            if validos["tmin"] and validos["tmax"] and tmin == tmax:
                tipo = "tmin==tmax"

            elif validos["tmean"] and validos["tmax"] and tmean == tmax:
                tipo = "tmean==tmax"

            elif validos["tmean"] and validos["tmin"] and tmean == tmin:
                tipo = "tmean==tmin"

            # --- Comparaciones tipo desigualdad ---
            elif validos["tmin"] and validos["tmax"] and tmax < tmin:
                tipo = "tmax<tmin"

            elif validos["tmean"] and validos["tmax"] and tmean > tmax:
                tipo = "tmean>tmax"

            elif validos["tmean"] and validos["tmin"] and tmean < tmin:
                tipo = "tmean<tmin"

            # Si no se detectó nada, omitir entrada
            if tipo is None:
                continue

            inconsistencias.append(
                {
                    "fecha": row["fecha"],
                    "tmin": float(tmin),
                    "tmean": float(tmean),
                    "tmax": float(tmax),
                    "tipo": tipo,
                }
            )

        except Exception:
            tipo = "indefinido"

    return inconsistencias


def _get_row_mask_by_fecha(df: pd.DataFrame, fecha: pd.Timestamp):
    return df["fecha"] == fecha


def apply_thermal_correction(
    action: str,
    fecha: pd.Timestamp,
    dfs: Dict[str, Optional[pd.DataFrame]],
    folder_out: str,
    estacion: str,
    periodo: str,
    nota: str = "",
) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Aplica la corrección térmica indicada por 'action' en la fecha dada.
    action ∈ {'i','t','x','e','s','m','r'}.
    dfs: dict con keys tmin,tmean,tmax (DataFrames o None).
    Devuelve dfs actualizados (no guardados).
    También registra en changes_applied.json una entrada de auditoría.
    """
    # Aseguramos dfs
    # Aseguramos dfs
    d_tmin = dfs.get("tmin")
    d_tmean = dfs.get("tmean")
    d_tmax = dfs.get("tmax")

    # --- Solución: evitar aliasing accidental ---
    # Forzamos copias profundas locales para que las modificaciones no propaguen
    # a otras referencias externas que pudieran apuntar al mismo DataFrame.
    for _k in ("tmin", "tmean", "tmax"):
        if dfs.get(_k) is not None:
            dfs[_k] = dfs[_k].copy(deep=True)

    # refrescar referencias locales
    d_tmin = dfs.get("tmin")
    d_tmean = dfs.get("tmean")
    d_tmax = dfs.get("tmax")

    # helper to get old values
    def _get_val(dframe):
        if dframe is None:
            return None
        m = _get_row_mask_by_fecha(dframe, fecha)
        if not m.any():
            return None
        return float(dframe.loc[m, "valor"].values[0])

    before = {
        "tmin": _get_val(d_tmin),
        "tmean": _get_val(d_tmean),
        "tmax": _get_val(d_tmax),
    }

    # Ensure rows exist for the date in each df; if missing, append row with -99
    for key, df in (("tmin", d_tmin), ("tmean", d_tmean), ("tmax", d_tmax)):
        if df is None:
            # create minimal df
            newdf = pd.DataFrame({"fecha": [fecha], "valor": [-99.0]})
            dfs[key] = newdf
        else:
            mask = _get_row_mask_by_fecha(df, fecha)
            if not mask.any():
                # append row (compatibilidad pandas moderna)
                df.loc[len(df)] = {"fecha": fecha, "valor": -99.0}
                df = df.sort_values("fecha").reset_index(drop=True)
                dfs[key] = df

    # refresh refs
    d_tmin = dfs.get("tmin")
    d_tmean = dfs.get("tmean")
    d_tmax = dfs.get("tmax")

    # Function to set value
    def _set_val(dframe, fecha_obj, newval):
        mask = dframe["fecha"] == fecha_obj
        if mask.any():
            dframe.loc[mask, "valor"] = newval
        else:
            dframe.loc[len(dframe)] = {"fecha": fecha_obj, "valor": newval}
        return dframe

    accion = action.lower().strip()

    # determine current values after ensuring date rows
    cur = {
        "tmin": _get_val(dfs.get("tmin")),
        "tmean": _get_val(dfs.get("tmean")),
        "tmax": _get_val(dfs.get("tmax")),
    }

    # Apply actions
    if accion == "m":
        # mantener sin cambios
        pass
    elif accion == "i":
        # intercambio tmax <-> tmin
        val_tmin = cur["tmin"]
        val_tmax = cur["tmax"]
        if val_tmin is None or val_tmax is None:
            # nothing to do
            pass
        else:
            dfs["tmin"] = _set_val(dfs["tmin"], fecha, val_tmax)
            dfs["tmax"] = _set_val(dfs["tmax"], fecha, val_tmin)
    elif accion == "t":
        # poner solo tmean en -99
        dfs["tmean"] = _set_val(dfs["tmean"], fecha, -99.0)
    elif accion == "x":
        # poner tmean y la variable afectada en -99
        # decide por comparación con tmax/tmin
        # if tmean > tmax -> set tmean and tmax to -99
        if (
            cur["tmean"] is not None
            and cur["tmax"] is not None
            and cur["tmean"] > cur["tmax"]
        ):
            dfs["tmean"] = _set_val(dfs["tmean"], fecha, -99.0)
            dfs["tmax"] = _set_val(dfs["tmax"], fecha, -99.0)
        elif (
            cur["tmean"] is not None
            and cur["tmin"] is not None
            and cur["tmean"] < cur["tmin"]
        ):
            dfs["tmean"] = _set_val(dfs["tmean"], fecha, -99.0)
            dfs["tmin"] = _set_val(dfs["tmin"], fecha, -99.0)
        else:
            # fallback: set tmean only
            dfs["tmean"] = _set_val(dfs["tmean"], fecha, -99.0)
    elif accion == "e":
        # edición manual: pedimos al usuario por consola los 3 valores
        try:
            inp_tmin = input(
                f"Nuevo tmin para {fecha.strftime('%Y-%m-%d')} (enter para mantener): "
            ).strip()
            inp_tmean = input(
                f"Nuevo tmean para {fecha.strftime('%Y-%m-%d')} (enter para mantener): "
            ).strip()
            inp_tmax = input(
                f"Nuevo tmax para {fecha.strftime('%Y-%m-%d')} (enter para mantener): "
            ).strip()
            if inp_tmin != "":
                v = float(inp_tmin)
                dfs["tmin"] = _set_val(dfs["tmin"], fecha, v)
            if inp_tmean != "":
                v = float(inp_tmean)
                dfs["tmean"] = _set_val(dfs["tmean"], fecha, v)
            if inp_tmax != "":
                v = float(inp_tmax)
                dfs["tmax"] = _set_val(dfs["tmax"], fecha, v)
        except Exception:
            print(
                "Valor inválido en edición manual. No se aplicaron cambios en la edición."
            )
    elif accion == "s":
        # sustituir los tres por -99
        dfs["tmin"] = _set_val(dfs["tmin"], fecha, -99.0)
        dfs["tmean"] = _set_val(dfs["tmean"], fecha, -99.0)
        dfs["tmax"] = _set_val(dfs["tmax"], fecha, -99.0)
    elif accion == "u":
        # poner solo tmax = -99
        dfs["tmax"] = _set_val(dfs["tmax"], fecha, -99.0)
    elif accion == "l":
        # poner solo tmin = -99
        dfs["tmin"] = _set_val(dfs["tmin"], fecha, -99.0)
    elif accion == "r":
        # reordenar automáticamente (manteniendo -99 fuera)
        vals = []
        for k in ("tmin", "tmean", "tmax"):
            v = cur.get(k)
            if v is not None and v != -99:
                vals.append(float(v))
        if len(vals) >= 2:
            vals_sorted = sorted(vals)
            # assign in order to tmin,tmean,tmax as available
            assign = {"tmin": None, "tmean": None, "tmax": None}
            if len(vals_sorted) == 2:
                assign["tmin"], assign["tmean"] = vals_sorted[0], vals_sorted[1]
                assign["tmax"] = cur["tmax"] if cur["tmax"] != -99 else vals_sorted[-1]
            else:
                assign["tmin"], assign["tmean"], assign["tmax"] = (
                    vals_sorted[0],
                    vals_sorted[1],
                    vals_sorted[2],
                )
            # set non-None
            for k in assign:
                if assign[k] is not None:
                    dfs[k] = _set_val(dfs[k], fecha, assign[k])
        else:
            # no hay suficientes valores para reordenar
            pass
    else:
        # acción desconocida: no hacer nada
        pass

    # Después de aplicar la corrección, solo aseguramos las fechas como datetime
    for k in ("tmin", "tmean", "tmax"):
        dfk = dfs.get(k)
        if dfk is not None:
            dfk["fecha"] = pd.to_datetime(dfk["fecha"])
            dfs[k] = dfk.sort_values("fecha").reset_index(drop=True)

    # register change in JSON for audit
    after = {
        "tmin": _get_val(dfs.get("tmin")),
        "tmean": _get_val(dfs.get("tmean")),
        "tmax": _get_val(dfs.get("tmax")),
    }

    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "estacion": estacion.upper(),
        "fecha": fecha.strftime("%Y-%m-%d"),
        "accion": accion,
        "nota": nota,
        "valores_previos": before,
        "valores_nuevos": after,
    }

    # append to single_changes list for audit
    changes = _load_changes(folder_out)
    changes.setdefault("single_changes", []).append(entry)
    _save_changes(folder_out, changes)

    return dfs


from qc_batch.io_manager import (
    write_tmp,
)  # agregar al inicio del archivo si no está importado


def write_triplet_tmp(
    dfs: Dict[str, pd.DataFrame],
    paths_trip: Dict[str, str],
    folder_out: str,
    estacion: str,
) -> Dict[str, str]:
    """
    Guarda los DataFrames tmin/tmean/tmax como *_tmp.csv en folder_out
    usando el período REAL de cada variable, inferido desde el archivo
    que fue cargado originalmente (paths_trip).
    Retorna dict {var: ruta_escrita}
    """
    paths = {}
    estacion_upper = estacion.upper()

    for var in ("tmin", "tmean", "tmax"):
        df = dfs.get(var)
        if df is None:
            continue

        src_path = paths_trip.get(var)
        if not src_path:
            raise ValueError(
                f"No hay ruta fuente registrada para {var}; "
                "no se puede determinar el período real."
            )

        periodo_real = extract_period_from_filename(src_path)
        if not periodo_real:
            raise ValueError(
                f"No se pudo determinar el período real para {var} desde {src_path}"
            )

        df_loc = df.copy()
        df_loc["fecha"] = pd.to_datetime(df_loc["fecha"], errors="coerce")
        df_loc["valor"] = pd.to_numeric(df_loc["valor"], errors="coerce")

        path_written = write_tmp(
            df_loc,
            folder_out,
            var,
            periodo_real,
            estacion_upper,
        )

        paths[var] = path_written

    return paths
