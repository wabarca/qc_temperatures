#!/usr/bin/env python3
"""
modifications.py

Módulo auxiliar para generar archivos de cambios asociados al QC.

Funciones:
 - cargar JSON de cambios
 - comparar df_org vs df_qc
 - clasificar cambios (termico / estadistico / ambos / ninguno)
 - generar DataFrame resumen
 - guardar archivo *_changes.csv
"""

import json
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import numpy as np


# -------------------------------------------------------------------
# Cargar archivo JSON de cambios
# -------------------------------------------------------------------
def load_changes_json(folder_out: str) -> Dict[str, Any]:
    path = Path(folder_out) / "changes_applied.json"
    if not path.exists():
        return {"single_changes": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"single_changes": []}


# -------------------------------------------------------------------
# Crear diccionario {fecha: tipo_cambio_termico}
# -------------------------------------------------------------------
def extract_thermal_changes(json_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Devuelve dict: { "YYYY-MM-DD": "termico" }
    basado en acciones del control termodinámico.
    """
    cambios = {}

    for entry in json_data.get("single_changes", []):
        fecha = entry.get("fecha")
        accion = entry.get("accion")

        # Acciones térmicas válidas
        if accion in ("i", "t", "x", "e", "s", "r"):
            cambios[fecha] = "termico"

    return cambios


# -------------------------------------------------------------------
# Crear diccionario {fecha: tipo_cambio_estadistico}
# -------------------------------------------------------------------
def extract_statistical_changes(
    df_org: pd.DataFrame, df_qc: pd.DataFrame
) -> Dict[str, str]:
    """
    Detecta cambios estadísticos comparando df_org vs df_qc.

    Si la fecha aparece en ambos:
      - valor org != valor qc -> estadistico
    """
    cambios = {}

    for fecha_org, val_org in zip(df_org["fecha"], df_org["valor"]):
        mask = df_qc["fecha"] == fecha_org
        if not mask.any():
            continue

        val_qc = df_qc.loc[mask, "valor"].values[0]

        # asegurar tipo numérico
        try:
            a = float(val_org)
        except Exception:
            a = np.nan
        try:
            b = float(val_qc)
        except Exception:
            b = np.nan

        if not np.isclose(a, b, equal_nan=True):
            cambios[fecha_org.strftime("%Y-%m-%d")] = "estadistico"

    return cambios


# -------------------------------------------------------------------
# Construir DataFrame resumen de cambios
# -------------------------------------------------------------------
def build_changes_dataframe(
    df_org: pd.DataFrame, df_qc: pd.DataFrame, folder_out: str
) -> pd.DataFrame:
    """
    Devuelve DF con columnas:
      fecha, valor_original, valor_qc, tipo_cambio, accion

    tipo_cambio ∈ { termico, estadistico, ambos, ninguno }
    """
    json_data = load_changes_json(folder_out)

    cambios_termicos = extract_thermal_changes(json_data)
    cambios_estad = extract_statistical_changes(df_org, df_qc)

    rows = []

    for fecha_org, val_org in zip(df_org["fecha"], df_org["valor"]):
        fecha_str = fecha_org.strftime("%Y-%m-%d")
        mask = df_qc["fecha"] == fecha_org

        if not mask.any():
            continue

        val_qc = df_qc.loc[mask, "valor"].values[0]

        # clasificar tipo de cambio
        t = fecha_str in cambios_termicos
        e = fecha_str in cambios_estad

        if t and e:
            tipo = "ambos"
        elif t:
            tipo = "termico"
        elif e:
            tipo = "estadistico"
        else:
            tipo = "ninguno"

        rows.append(
            {
                "fecha": fecha_str,
                "valor_original": val_org,
                "valor_qc": val_qc,
                "tipo_cambio": tipo,
            }
        )

    df = pd.DataFrame(rows)
    df = df[df["tipo_cambio"] != "ninguno"]
    return df


# -------------------------------------------------------------------
# Guardar archivo de cambios
# -------------------------------------------------------------------
def save_changes_csv(
    df_changes: pd.DataFrame, folder_out: str, var: str, periodo: str, estacion: str
) -> str:
    """
    Guarda archivo *_changes.csv asociado al QC.
    """
    estacion = estacion.upper()
    fname = f"{var}_{periodo}_{estacion}_changes.csv"
    path = Path(folder_out) / fname

    df_changes.to_csv(path, index=False, encoding="utf-8")

    return str(path)
