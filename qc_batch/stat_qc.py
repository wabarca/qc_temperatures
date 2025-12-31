#!/usr/bin/env python3
"""
stat_qc.py

Módulo estadístico para:
 - calcular percentiles, IQR y límites
 - detectar outliers
 - aplicar sustituciones simples (opcional)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import json
from pathlib import Path


def compute_bounds(
    series: pd.Series, lower_p: float = 0.10, upper_p: float = 0.90, k: float = 1.5
) -> Dict[str, float]:
    """
    Calcula p_low, p_high, IQR y límites inferior/superior.

    series: Serie de valores (float), excluyendo -99 antes de llamar.

    lower_p: percentil inferior (ej: 0.10)
    upper_p: percentil superior (ej: 0.90)
    k: multiplicador del IQR (ej: 1.5)

    Retorna dict:
      {
        "p_low": ...,
        "p_high": ...,
        "iqr": ...,
        "lim_inf": ...,
        "lim_sup": ...
      }
    """

    if len(series) == 0:
        return {
            "p_low": np.nan,
            "p_high": np.nan,
            "iqr": np.nan,
            "lim_inf": np.nan,
            "lim_sup": np.nan,
        }

    p_low = np.nanpercentile(series, lower_p * 100)
    p_high = np.nanpercentile(series, upper_p * 100)
    iqr = p_high - p_low

    lim_inf = p_low - k * iqr
    lim_sup = p_high + k * iqr

    return {
        "p_low": float(p_low),
        "p_high": float(p_high),
        "iqr": float(iqr),
        "lim_inf": float(lim_inf),
        "lim_sup": float(lim_sup),
    }


def get_validated_outliers(folder_out):
    """
    Retorna conjunto de fechas validadas por el usuario (accion == 'm'),
    indexadas SOLO por estación y fecha.
    """
    try:
        data = json.loads(
            Path(folder_out, "changes_applied.json").read_text(encoding="utf-8")
        )

        return {
            (entry.get("estacion"), entry.get("fecha"))
            for entry in data.get("single_changes", [])
            if entry.get("accion") == "m"
            and entry.get("estacion") is not None
            and entry.get("fecha") is not None
        }
    except Exception:
        return set()


def detect_outliers(
    df: pd.DataFrame,
    bounds: Dict[str, float],
    folder_out: str,
    estacion: str,
    variable: str,
    col_val: str = "valor",
) -> List[Tuple[int, float]]:
    """
    Detecta outliers estadísticos en un DataFrame.

    Un outlier previamente validado (accion == 'm') para la misma
    estación, variable y fecha NO vuelve a detectarse.
    """

    if df is None or df.empty or col_val not in df.columns:
        return []

    lim_inf = bounds.get("lim_inf")
    lim_sup = bounds.get("lim_sup")

    # Validación básica de límites
    if lim_inf is None or lim_sup is None:
        return []

    if not np.isfinite(lim_inf) or not np.isfinite(lim_sup):
        return []

    # Rango degenerado → no detectar
    if (lim_sup - lim_inf) <= 1e-6:
        return []

    validated = get_validated_outliers(folder_out)

    outliers = []

    for i, v in enumerate(df[col_val]):
        if v == -99:
            continue

        fecha = df.loc[i, "fecha"]
        if pd.isna(fecha):
            continue

        fecha_str = pd.to_datetime(fecha).strftime("%Y-%m-%d")
        key = (estacion, fecha_str)

        # 🔒 Outlier ya validado
        if key in validated:
            continue

        if v < lim_inf or v > lim_sup:
            outliers.append((i, float(v)))

    return outliers


def apply_statistical_decision(
    df: pd.DataFrame,
    index: int,
    decision: str,
    new_value: float = None,
    col_val: str = "valor",
) -> pd.DataFrame:
    """
    Aplica la decisión 's', 'm', 'n' a un solo outlier.

    s -> sustituir por -99
    m -> mantener
    n -> reemplazar por new_value

    Retorna df actualizado.
    """

    if decision == "s":
        df.loc[index, col_val] = -99
    elif decision == "n" and new_value is not None:
        df.loc[index, col_val] = float(new_value)
    # decision == "m" → no cambia nada

    return df
