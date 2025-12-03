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


def detect_outliers(
    df: pd.DataFrame, bounds: Dict[str, float], col_val: str = "valor"
) -> List[Tuple[int, float]]:
    """
    Detecta outliers en un DataFrame que debe tener columna 'valor'.

    Retorna lista de tuplas:
      [(indice, valor), ...]

    Solo detecta outliers donde valor != -99.
    """

    lim_inf = bounds.get("lim_inf")
    lim_sup = bounds.get("lim_sup")

    if np.isnan(lim_inf) or np.isnan(lim_sup):
        return []

    outliers = []

    for i, v in enumerate(df[col_val]):
        if v == -99:
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
