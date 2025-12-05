#!/usr/bin/env python3
"""
visualization.py — Versión final modernizada (2025)
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib as mpl

# ======================================================
# ESTILO GLOBAL MODERNO
# ======================================================

mpl.rcParams.update(
    {
        "font.family": "Calibri",
        "font.size": 10,
        "axes.titlesize": 13,
        "axes.labelsize": 11,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "figure.titlesize": 15,
        "legend.fontsize": 9,
    }
)

# Paleta moderna por variable
COLOR_VAR = {
    "tmax": "#D62728",  # rojo
    "tmean": "#7F7F7F",  # gris neutro
    "tmin": "#1F77B4",  # azul
    "pr": "#8CB5FF",  # azul claro
}

# ============================================================
# PANEL VACÍO
# ============================================================


def _draw_empty_panel(ax):
    ax.set_facecolor("#FAFAFA")
    ax.grid(True, linestyle="--", alpha=0.35)
    for side in ["top", "bottom", "left", "right"]:
        ax.spines[side].set_visible(True)
    ax.text(
        0.5,
        0.5,
        "Sin datos",
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=11,
        color="gray",
    )


# ============================================================
# CONTEXTO 2×2
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
    show_labels=True,
):

    fecha_obj = pd.to_datetime(fecha_obj)
    var_principal = var_principal.lower()

    variables = ["tmax", "tmean", "tmin", "pr"]
    unidades = {"tmax": "°C", "tmean": "°C", "tmin": "°C", "pr": "mm"}

    fig, axes = plt.subplots(2, 2, figsize=(12, 7), dpi=110)
    axes = axes.flatten()

    locator = mdates.DayLocator()
    formatter = mdates.DateFormatter("%d-%b")

    start = fecha_obj - pd.Timedelta(days=ventana)
    end = fecha_obj + pd.Timedelta(days=ventana)

    # -----------------------------------
    # Determinar variables anómalas
    # -----------------------------------
    vars_anomalas = set()

    if tipo_inconsistencia == "estadistico":
        vars_anomalas.add(var_principal)

    if tipo_inconsistencia:
        if "==tmin" in tipo_inconsistencia:
            vars_anomalas |= {"tmean", "tmin"}
        if "==tmax" in tipo_inconsistencia:
            vars_anomalas |= {"tmean", "tmax"}
        if tipo_inconsistencia == "tmean>tmax":
            vars_anomalas |= {"tmean", "tmax"}
        if tipo_inconsistencia == "tmean<tmin":
            vars_anomalas |= {"tmean", "tmin"}
        if tipo_inconsistencia == "tmax<tmin":
            vars_anomalas |= {"tmax", "tmin"}

    if not vars_anomalas:
        vars_anomalas.add(var_principal)

    # -----------------------------------
    # PANEL POR PANEL
    # -----------------------------------
    for ax, var in zip(axes, variables):

        ax.set_facecolor("#FAFAFA")
        ax.set_title(f"{var.title()} ({unidades[var]})", fontweight="bold", pad=14)
        ax.grid(True, linestyle="--", alpha=0.35)

        # Asegurar spines visibles arriba y abajo
        for side in ["top", "bottom", "left", "right"]:
            ax.spines[side].set_visible(True)

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
        valores = sub["valor"].astype(float).values

        # Validos vs faltantes
        vals = np.where(valores == -99, np.nan, valores)
        mask_missing = valores == -99

        # -----------------------------------
        # Límites de eje Y
        # -----------------------------------
        if var == "pr":
            vmax = np.nanmax(vals)
            ymax = vmax + max(1, 0.15 * vmax)
            ymin = -0.1 * ymax
            ax.set_ylim(ymin, ymax)
        else:
            vmin = np.nanmin(vals)
            vmax = np.nanmax(vals)
            rango = (vmax - vmin) if vmax > vmin else 1
            margen = 0.2 * rango
            ax.set_ylim(vmin - margen, vmax + margen)

        # -----------------------------------
        # Gráfica
        # -----------------------------------
        col = COLOR_VAR[var]

        if var == "pr":
            ax.bar(fechas, vals, width=0.6, color=col, alpha=0.45)
            ax.plot(fechas, vals, "-o", markersize=3, color="#5A9FFF", lw=1.0)
        else:
            ax.plot(fechas, vals, "-o", markersize=3, lw=1.0, color=col)

        # -----------------------------------
        # Faltantes -99 como círculos huecos
        # -----------------------------------
        if np.any(mask_missing):
            y_missing = np.full(mask_missing.sum(), ax.get_ylim()[0] + 0.05)
            ax.scatter(
                fechas[mask_missing],
                y_missing,
                facecolors="none",
                edgecolors="#D62728",
                s=30,
                linewidths=1.3,
                marker="o",
                zorder=10,
            )

        # -----------------------------------
        # Etiquetas numéricas
        # -----------------------------------
        if show_labels:
            yoff = 0.05 * (ax.get_ylim()[1] - ax.get_ylim()[0])
            for f, v in zip(fechas, vals):
                if np.isnan(v):
                    continue
                txt = str(v).rstrip("0").rstrip(".")
                ax.text(
                    f,
                    v + yoff,
                    txt,
                    fontsize=8,
                    ha="center",
                    bbox=dict(
                        facecolor="white",
                        edgecolor="black",
                        boxstyle="round,pad=0.2",
                        alpha=0.55,
                    ),
                )

        # -----------------------------------
        # Anomalía
        # -----------------------------------
        mask_fecha = sub["fecha"] == fecha_obj
        if var in vars_anomalas and mask_fecha.any():
            v = sub.loc[mask_fecha, "valor"].values[0]
            if v != -99:
                ax.scatter(
                    [fecha_obj],
                    [v],
                    s=50,
                    color="red",
                    edgecolor="black",
                    linewidth=1.0,
                    zorder=12,
                )

        # Eje X
        ax.axvline(fecha_obj, color="gray", linestyle="--", linewidth=1)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
        ax.tick_params(axis="x", rotation=45)
        ax.set_xlim(start, end)

    fig.subplots_adjust(hspace=0.32)
    fig.suptitle(
        f"Contexto: anomalía en {var_principal.title()} – estación {estacion.upper()} – {fecha_obj:%Y-%m-%d}",
        fontweight="bold",
    )

    fig.tight_layout(rect=[0, 0, 1, 0.95])

    if folder_out:
        outdir = Path(folder_out) / "fig_contexto"
        outdir.mkdir(exist_ok=True)
        fname = f"anomalia_{estacion.upper()}_{var_principal}_{fecha_obj:%Y%m%d}.png"
        fig.savefig(outdir / fname, dpi=140, bbox_inches="tight")

    if show:
        plt.show(block=False)

    return fig


# ============================================================
# COMPARACIÓN ORG VS QC
# ============================================================


def plot_comparison_qc(df_org, df_qc, var, periodo, estacion, folder_out):

    fig, axes = plt.subplots(2, 1, figsize=(12, 7), dpi=110)

    df_org = df_org.copy()
    df_qc = df_qc.copy()
    df_org["fecha"] = pd.to_datetime(df_org["fecha"])
    df_qc["fecha"] = pd.to_datetime(df_qc["fecha"])

    fechas = df_org["fecha"].values
    vo = df_org["valor"].astype(float).replace(-99, np.nan).values
    vq = df_qc["valor"].astype(float).replace(-99, np.nan).values

    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)

    # -----------------------------
    # Panel 1: Serie original
    # -----------------------------
    ax = axes[0]
    ax.set_facecolor("#FAFAFA")
    ax.grid(True, linestyle="--", alpha=0.35)
    for side in ["top", "bottom", "left", "right"]:
        ax.spines[side].set_visible(True)

    ax.plot(fechas, vo, "-", lw=0.8, color=COLOR_VAR.get(var, "blue"))
    ax.set_title("Serie Original", pad=14)

    # -----------------------------
    # Panel 2: Serie corregida
    # -----------------------------
    ax2 = axes[1]
    ax2.set_facecolor("#FAFAFA")
    ax2.grid(True, linestyle="--", alpha=0.35)
    for side in ["top", "bottom", "left", "right"]:
        ax2.spines[side].set_visible(True)

    ax2.plot(fechas, vq, "-", lw=0.8, color=COLOR_VAR.get(var, "blue"))

    # Valores modificados
    mask_mod = ~np.isclose(vo, vq, equal_nan=True)
    if np.any(mask_mod):
        ax2.scatter(
            fechas[mask_mod],
            vq[mask_mod],
            s=50,
            marker="D",
            color="#7B61FF",
            edgecolor="black",
            linewidth=1.0,
            label="Modificado",
            zorder=6,
        )

    # Faltantes
    mask_missing = df_qc["valor"] == -99
    if np.any(mask_missing):
        yvals = vq[~np.isnan(vq)]
        if yvals.size > 0:
            ymin = np.nanmin(yvals)
            ymax = np.nanmax(yvals)
            rng = max(1, ymax - ymin)
            y_offset = ymin - 0.1 * rng
        else:
            y_offset = -1

        ax2.scatter(
            fechas[mask_missing],
            [y_offset] * mask_missing.sum(),
            s=50,
            marker="o",
            facecolors="none",
            edgecolors="#D62728",
            linewidths=1.3,
            label="Faltante (-99)",
            zorder=7,
        )

    ax2.set_title("Serie Corregida", pad=14)
    ax2.legend()

    ax2.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)
    ax2.tick_params(axis="x", rotation=35)

    fig.tight_layout()

    outdir = Path(folder_out)
    outdir.mkdir(parents=True, exist_ok=True)
    fname = f"{var}_{periodo}_{estacion.upper()}_comparacion.png"
    fig.savefig(outdir / fname, dpi=140, bbox_inches="tight")

    plt.close(fig)
    return str(outdir / fname)


def plot_image_preview(path_png: str):
    try:
        img = plt.imread(path_png)
        plt.figure(figsize=(8, 4))
        plt.imshow(img)
        plt.axis("off")
        plt.show(block=False)
    except Exception as e:
        print(f"No se puede previsualizar imagen: {e}")
