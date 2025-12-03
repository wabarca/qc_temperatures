#!/usr/bin/env python3
"""
helpers_compare.py

Comparación interactiva con otras estaciones (estilo código original),
con fallback flexible de período para TODAS las variables (tmax, tmin, tmean, pr).
"""

from pathlib import Path
import pandas as pd
from qc_batch.io_manager import find_candidate_file, read_series
from qc_batch.visualization import plot_context_2x2


def find_series_for_station_full_fallback(
    folder_in, folder_out, var, periodo_actual, estacion_comp
):
    """
    Búsqueda ampliada de series para comparación visual entre estaciones.

    PRIORIDAD:
    1) *_tmp.csv   (output)
    2) *_QC.csv    (output)
    3) *_org.csv   con periodo exacto (input)
    4) var_*_estacion*_org.csv (fallback flexible)
    5) var_*_estacion*.csv     (fallback universal)

    Devuelve: ruta al archivo encontrado o None.
    """

    folder_in = Path(folder_in)
    folder_out = Path(folder_out)

    # ----------------------------
    # 1. Intentar match exacto usando find_candidate_file
    # ----------------------------
    info = find_candidate_file(
        folder_in, folder_out, var, periodo_actual, estacion_comp
    )
    path = info.get("path")

    if path and Path(path).exists():
        return path

    # ----------------------------
    # 2. Fallback flexible período (solo org)
    # tmax_*_T-06_org.csv
    # ----------------------------
    pattern_org = f"{var}_*_{estacion_comp}_org.csv"
    matches_org = sorted(folder_in.glob(pattern_org))
    if matches_org:
        print(
            f"[FALLBACK] Usando archivo {matches_org[0].name} (org) para {var.upper()} en estación {estacion_comp}"
        )
        return str(matches_org[0])

    # ----------------------------
    # 3. Fallback universal
    # tmax_*_T-06*.csv
    # ----------------------------
    pattern_any = f"{var}_*_{estacion_comp}*.csv"
    matches_any = sorted(folder_in.glob(pattern_any))
    if matches_any:
        print(
            f"[FALLBACK] Usando archivo {matches_any[0].name} para {var.upper()} en estación {estacion_comp}"
        )
        return str(matches_any[0])

    # ----------------------------
    # 4. No encontrado
    # ----------------------------
    return None


def compare_with_other_station(
    var: str,
    periodo: str,
    estacion_actual: str,
    fecha_obj,
    folder_in: str,
    folder_out: str,
    ventana: int,
    ask_user=input,
):
    """
    Flujo interactivo para comparación con otras estaciones,
    usando fallback flexible para cualquier período.
    """

    fecha_obj = pd.to_datetime(fecha_obj)

    while True:
        resp = (
            ask_user("¿Desea comparar con otra estación para el mismo día? (s/n): ")
            .strip()
            .lower()
        )
        if resp not in ("s", "y"):
            return

        estacion_comp = ask_user("Ingrese el ID de la estación (ej. S-12): ").strip()
        if not estacion_comp:
            print("⚠ Código vacío.")
            continue

        # ---------------------------------------
        # Buscar archivo equivalente con fallback
        # ---------------------------------------
        path = find_series_for_station_full_fallback(
            folder_in, folder_out, var, periodo, estacion_comp
        )

        if not path or not Path(path).exists():
            print(
                f"⚠ No se encontró archivo para {var.upper()} en estación {estacion_comp}."
            )
            continue

        # Cargar serie auxiliar
        df_aux = read_series(path).copy()
        df_aux["fecha"] = pd.to_datetime(df_aux["fecha"])

        # Encontrar fecha más cercana
        idx = (df_aux["fecha"] - fecha_obj).abs().idxmin()
        fecha_real = df_aux.loc[idx, "fecha"]

        # =====================================================
        # Cargar TODAS las variables de la estación auxiliar
        # =====================================================
        vars_all = ["tmax", "tmean", "tmin", "pr"]
        dfs = {}

        for v in vars_all:
            path_v = find_series_for_station_full_fallback(
                folder_in, folder_out, v, periodo, estacion_comp
            )
            if path_v and Path(path_v).exists():
                try:
                    dfv = read_series(path_v).copy()
                    dfv["fecha"] = pd.to_datetime(dfv["fecha"])
                    dfs[v] = dfv
                except Exception as e:
                    print(f"⚠ Error leyendo {v} de estación {estacion_comp}: {e}")
                    dfs[v] = None
            else:
                dfs[v] = None

        # =====================================================
        # Mostrar comparativa con las 4 variables
        # =====================================================
        fig_aux = plot_context_2x2(
            dfs,
            var_principal=var,
            estacion=estacion_comp,
            fecha_obj=fecha_real,
            ventana=ventana,
            folder_out=None,
            show=True,
        )

        print(f"🟢 Comparativa abierta para estación {estacion_comp}.\n")

        # Devolver figura auxiliar para que workflow pueda cerrarla
        return fig_aux

    return None
