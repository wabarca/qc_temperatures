#!/usr/bin/env python3
"""
visualization.py — Versión corregida y estabilizada
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib as mpl

mpl.rcParams["font.family"] = "Calibri"
mpl.rcParams["font.size"] = 9
mpl.rcParams["axes.titlesize"] = 12
mpl.rcParams["axes.labelsize"] = 10
mpl.rcParams["xtick.labelsize"] = 8
mpl.rcParams["ytick.labelsize"] = 8
mpl.rcParams["figure.titlesize"] = 14
mpl.rcParams["legend.fontsize"] = 8


# ============================================================
# CONTEXTO 2x2 — TMAX / TMEAN / TMIN / PR
# ============================================================


def plot_context_2x2(
    dfs,
    var_principal,
    estacion,
    fecha_obj,
    ventana,
    tipo_inconsistencia=None,
    folder_out=None,
    show=True,
):

    var_principal = var_principal.lower()
    fecha_obj = pd.to_datetime(fecha_obj)

    variables = ["tmax", "tmean", "tmin", "pr"]
    unidades = {"tmax": "°C", "tmean": "°C", "tmin": "°C", "pr": "mm"}

    fig, axes = plt.subplots(2, 2, figsize=(12, 8), dpi=120)
    axes = axes.flatten()

    locator = mdates.DayLocator()
    formatter = mdates.DateFormatter("%d-%b")

    start = fecha_obj - pd.Timedelta(days=ventana)
    end = fecha_obj + pd.Timedelta(days=ventana)

    # ---------------------------------------------------
    # Determinar variables afectadas según inconsistencia
    # ---------------------------------------------------
    vars_anomalas = set()

    if tipo_inconsistencia == "estadistico":
        vars_anomalas = {var_principal}

    if tipo_inconsistencia:
        if "==tmin" in tipo_inconsistencia or tipo_inconsistencia in (
            "tmean==tmin",
            "tmin==tmean",
        ):
            vars_anomalas.add("tmean")
            vars_anomalas.add("tmin")
        if "==tmax" in tipo_inconsistencia or tipo_inconsistencia in (
            "tmean==tmax",
            "tmax==tmean",
        ):
            vars_anomalas.add("tmean")
            vars_anomalas.add("tmax")
        if tipo_inconsistencia == "tmean>tmax":
            vars_anomalas.add("tmean")
            vars_anomalas.add("tmax")
        if tipo_inconsistencia == "tmean<tmin":
            vars_anomalas.add("tmean")
            vars_anomalas.add("tmin")
        if tipo_inconsistencia == "tmax<tmin":
            vars_anomalas.add("tmax")
            vars_anomalas.add("tmin")

    # fallback: si no detectó nada, usar var_principal
    if not vars_anomalas:
        vars_anomalas.add(var_principal)

    # ----------------------------
    # PANEL POR PANEL
    # ----------------------------
    for ax, var in zip(axes, variables):
        ax.set_title(f"{var.title()} ({unidades[var]})", fontweight="bold")
        ax.grid(True, linestyle="--", alpha=0.4)

        df = dfs.get(var)
        if df is None or df.empty:
            _draw_empty_panel(ax)
            continue

        d = df.copy()
        d["fecha"] = pd.to_datetime(d["fecha"])
        d = d.sort_values("fecha")
        sub = d[(d["fecha"] >= start) & (d["fecha"] <= end)]

        if sub.empty:
            _draw_empty_panel(ax)
            continue

        fechas = sub["fecha"].values
        vals = sub["valor"].replace(-99, np.nan).astype(float).values

        if np.all(np.isnan(vals)):
            _draw_empty_panel(ax)
            continue

        # Límites
        if var == "pr":
            vmax = np.nanmax(vals)
            top_margin = 0.2 * (vmax if vmax > 0 else 1)
            bottom_margin = -0.15 * (vmax + 1)
            ax.set_ylim(bottom_margin, vmax + top_margin)
        else:
            vmin = np.nanmin(vals)
            vmax = np.nanmax(vals)
            rango = vmax - vmin if vmax > vmin else 1
            margin = 0.2 * rango
            ax.set_ylim(vmin - margin, vmax + margin)

        # Gráficas
        if var == "pr":
            ax.bar(fechas, np.nan_to_num(vals), width=0.8, color="#3A70E0", alpha=0.6)
            ax.plot(fechas, np.nan_to_num(vals), "-", color="#3A70E0", lw=1, alpha=0.7)
            ax.scatter(fechas, np.nan_to_num(vals), color="#3A70E0", s=15, alpha=0.9)
        else:
            ax.plot(fechas, vals, "-o", markersize=5, lw=1.2, color="#3A70E0")

        # Línea vertical
        ax.axvline(fecha_obj, color="gray", linestyle="--", linewidth=1)

        # Etiquetas
        for f, v in zip(fechas, vals):
            if pd.isna(v):
                continue
            yoff = 0.03 * (ax.get_ylim()[1] - ax.get_ylim()[0])
            text_val = str(v).rstrip("0").rstrip(".")
            ax.text(
                f,
                v + yoff,
                text_val,
                ha="center",
                va="bottom",
                fontsize=7,
                color="black",
                bbox=dict(
                    facecolor="white",
                    edgecolor="black",
                    boxstyle="round,pad=0.2",
                    alpha=0.55,
                ),
            )

        # Marcar anómalo
        mask_fecha = sub["fecha"] == fecha_obj
        # Marcar valor anómalo SOLO si esta variable está involucrada
        if var in vars_anomalas and mask_fecha.any():
            val = sub.loc[mask_fecha, "valor"].values[0]
            if val != -99 and not pd.isna(val):
                ax.scatter(
                    [fecha_obj], [val], s=75, color="red", edgecolor="black", zorder=5
                )

        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.set_xlim(start, end)

    fig.suptitle(
        f"Contexto: anomalía en {var_principal.title()} "
        f"para {estacion.upper()} el {fecha_obj.strftime('%Y-%m-%d')}",
        fontweight="bold",
        fontsize=14,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    if folder_out:
        outdir = Path(folder_out) / "fig_contexto"
        outdir.mkdir(exist_ok=True)
        fname = (
            f"anomalia_{estacion.upper()}_{var_principal}_"
            f"{fecha_obj.strftime('%Y%m%d')}.png"
        )
        fig.savefig(outdir / fname, dpi=160, bbox_inches="tight")

    # ❗ Mostrar solo si se pide
    if show:
        plt.show(block=False)

    return fig


# ============================================================
# PANEL VACÍO CONSISTENTE
# ============================================================


def _draw_empty_panel(ax):
    ax.set_xlim(-1, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.text(
        0.5,
        0.5,
        "Sin datos",
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=10,
        color="gray",
    )


# ============================================================
# COMPARACIÓN ORG vs QC (2×1)
# ============================================================


def plot_comparison_qc(df_org, df_qc, var, periodo, estacion, folder_out):
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), dpi=120, sharex=True)

    df_org = df_org.copy()
    df_qc = df_qc.copy()

    df_org["fecha"] = pd.to_datetime(df_org["fecha"])
    df_qc["fecha"] = pd.to_datetime(df_qc["fecha"])

    fechas = df_org["fecha"].values
    vo = df_org["valor"].replace(-99, np.nan).astype(float).values
    vq = df_qc["valor"].replace(-99, np.nan).astype(float).values

    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)

    # Panel original
    axes[0].plot(fechas, vo, "-", lw=0.5, color="blue", label="Original")
    axes[0].set_title("Serie Original")
    axes[0].grid(True, linestyle="--", alpha=0.4)
    axes[0].legend()

    # Panel corregido
    axes[1].plot(fechas, vq, "-", lw=0.5, color="blue", label="Corregida")

    mask_mod = ~np.isclose(vo, vq, equal_nan=True)
    if np.any(mask_mod):
        axes[1].scatter(
            fechas[mask_mod],
            vq[mask_mod],
            s=80,
            facecolors="none",
            edgecolors="red",
            linewidths=1.4,
            label="Modificado",
            zorder=5,
        )

    axes[1].set_title("Serie Corregida")
    axes[1].grid(True, linestyle="--", alpha=0.4)
    axes[1].legend()

    axes[1].xaxis.set_major_locator(locator)
    axes[1].xaxis.set_major_formatter(formatter)
    axes[1].tick_params(axis="x", rotation=35)

    fig.tight_layout()

    outdir = Path(folder_out)
    outdir.mkdir(parents=True, exist_ok=True)
    fname = f"{var}_{periodo}_{estacion.upper()}_comparacion.png"
    fig.savefig(outdir / fname, dpi=160, bbox_inches="tight")

    plt.close(fig)
