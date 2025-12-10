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

# ===== SOPORTE MULTI-MONITOR (Windows) =====
try:
    import win32gui
    from screeninfo import get_monitors
except ImportError:
    win32gui = None
    get_monitors = None

# ======================================================
# DETECCIÓN DE MONITOR Y REUBICACIÓN DE FIGURA
# ======================================================


def detectar_monitor_activo():
    """
    Devuelve (indice_monitor, objeto_monitor) donde está la ventana activa (VSCode).
    Si no es posible detectar, devuelve (None, None).
    """
    if win32gui is None or get_monitors is None:
        return None, None

    try:
        hwnd = win32gui.GetForegroundWindow()
        rect = win32gui.GetWindowRect(hwnd)
        x_centro = (rect[0] + rect[2]) // 2
        y_centro = (rect[1] + rect[3]) // 2

        monitores = get_monitors()

        for i, m in enumerate(monitores):
            if (m.x <= x_centro <= m.x + m.width) and (
                m.y <= y_centro <= m.y + m.height
            ):
                return i, m
    except:
        pass

    return None, None


def obtener_monitor_opuesto():
    """
    Devuelve el monitor opuesto al que actualmente contiene VSCode.
    """
    if get_monitors is None:
        return None

    idx, _ = detectar_monitor_activo()
    monitores = get_monitors()

    if idx is None or not monitores:
        return None

    # Si solo hay un monitor → no hacemos nada
    if len(monitores) == 1:
        return monitores[0]  # el único monitor disponible

    # Si hay 2 → devolver el otro
    if len(monitores) == 2:
        return monitores[1 - idx]

    # Si hay más → elegir el primero que no sea el actual
    for i, m in enumerate(monitores):
        if i != idx:
            return m

    return None


def mover_figura_a_monitor_opuesto(manager):
    """
    Mueve la ventana de Matplotlib al monitor opuesto al de la terminal/VSCode.
    """
    monitor = obtener_monitor_opuesto()
    if monitor is None:
        return

    # Colocar ventana un poco dentro del monitor
    x = monitor.x  # + 100
    y = monitor.y  # + 100

    # Para backend TkAgg
    try:
        manager.window.wm_geometry(f"+{x}+{y}")
        return
    except:
        pass

    # Para backend Qt5Agg
    try:
        manager.window.move(x, y)
        return
    except:
        pass


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

    # No regenerar ticks ni grid: respetar los ejes ya definidos
    # Mantener spines visibles
    for side in ["top", "bottom", "left", "right"]:
        ax.spines[side].set_visible(True)

    # Mostrar mensaje centrado
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


# Reemplazar la función plot_context_2x2 por esta versión
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
    verbose=False,
):

    fecha_obj = pd.to_datetime(fecha_obj).normalize()
    var_principal = var_principal.lower()

    variables = ["tmax", "tmean", "tmin", "pr"]
    unidades = {"tmax": "°C", "tmean": "°C", "tmin": "°C", "pr": "mm"}

    fig, axes = plt.subplots(2, 2, figsize=(12, 7), dpi=110)
    axes = axes.flatten()

    locator = mdates.DayLocator()
    formatter = mdates.DateFormatter("%d-%b")

    start = fecha_obj - pd.Timedelta(days=ventana)
    end = fecha_obj + pd.Timedelta(days=ventana)

    # --------------------------------------------------------
    # Calcular límites globales por tipo para asegurar ejes
    # cuando un panel quede vacío en la ventana
    # --------------------------------------------------------
    global_limits = {}

    # Temperaturas: combinar tmin,tmean,tmax si existen (usamos todo el df, no solo la ventana)
    temps = []
    for tv in ("tmin", "tmean", "tmax"):
        dfv = dfs.get(tv)
        if dfv is not None and not dfv.empty:
            vals_all = pd.to_numeric(
                dfv["valor"].replace(-99, np.nan), errors="coerce"
            ).dropna()
            if not vals_all.empty:
                temps.extend(vals_all.tolist())
    if temps:
        global_vmin = float(np.nanmin(temps))
        global_vmax = float(np.nanmax(temps))
        rango = max(1.0, global_vmax - global_vmin)
        margen = 0.2 * rango
        global_limits["temp"] = (global_vmin - margen, global_vmax + margen)
    else:
        # fallback razonable para temperatura
        global_limits["temp"] = (10.0, 35.0)

    # Precipitación: buscar vmax global en pr si existe
    pr_df = dfs.get("pr")
    if pr_df is not None and not pr_df.empty:
        pr_vals = pd.to_numeric(
            pr_df["valor"].replace(-99, np.nan), errors="coerce"
        ).dropna()
        if not pr_vals.empty:
            vmax_pr = float(np.nanmax(pr_vals))
            ymax = vmax_pr + max(1.0, 0.15 * vmax_pr)
            global_limits["pr"] = (0.0, ymax)
        else:
            global_limits["pr"] = (0.0, 1.0)
    else:
        global_limits["pr"] = (0.0, 1.0)

    # -----------------------------------
    # Determinar variables anómalas
    # -----------------------------------
    vars_anomalas = set()

    if tipo_inconsistencia == "estadistico":
        if (
            var_principal != "pr"
        ):  # PR no se considera variable evaluable para estadística
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
            # establecer ejes temporales y verticales coherentes
            ax.set_xlim(start, end)
            if var == "pr":
                ymin, ymax = global_limits["pr"]
                ax.set_ylim(ymin, ymax)
            else:
                ymin, ymax = global_limits["temp"]
                ax.set_ylim(ymin, ymax)
            _draw_empty_panel(ax)
            continue

        d = df.copy()
        # Normalizar fecha a día (evita problemas por horas / tz)
        d["fecha"] = pd.to_datetime(d["fecha"]).dt.normalize()
        d = d.sort_values("fecha").reset_index(drop=True)

        # Filtrar por ventana
        sub = d[(d["fecha"] >= start) & (d["fecha"] <= end)]
        if sub.empty:

            # 1. Fijar límites de eje X
            ax.set_xlim(start, end)

            # 2. Fijar límites de eje Y según variable
            if var == "pr":
                ymin, ymax = global_limits["pr"]
            else:
                ymin, ymax = global_limits["temp"]

            ax.set_ylim(ymin, ymax)

            # 3. Aplicar SIEMPRE los mismos LOCATORS y FORMATTERS
            #    Esto corrige las ETIQUETAS del eje horizontal
            ax.xaxis.set_major_locator(locator)  # DayLocator()
            ax.xaxis.set_major_formatter(formatter)  # "%d-%b"
            ax.tick_params(axis="x", rotation=45)

            # 4. Redibujar panel vacío sin modificar ejes ya fijados
            _draw_empty_panel(ax)
            continue

        fechas = sub["fecha"].values
        # asegurar numeric
        valores = pd.to_numeric(sub["valor"], errors="coerce").astype(float).values

        # Validos vs faltantes: usar np.nan para -99
        vals = np.where(valores == -99, np.nan, valores)
        mask_missing = np.isnan(vals)

        # -----------------------------------
        # Proteger contra series vacías o sin valores válidos
        # -----------------------------------
        if np.all(np.isnan(vals)):
            _draw_empty_panel(ax)
            ax.set_title(f"{var.title()} (sin datos en ventana)", pad=14)
            continue

        # -----------------------------------
        # Límites de eje Y (FIJAR ANTES DE DIBUJAR)
        # -----------------------------------
        if var == "pr":
            vmax = np.nanmax(vals)
            # Evitar NaN en caso extremo de que todas las lluvias sean -99
            vmax = vmax if not np.isnan(vmax) else 1.0

            # Margen superior como en la versión original (muy estable)
            ymax = vmax + max(1.0, 0.15 * vmax)

            # Margen inferior suave y estable (igual que antes)
            ymin = -0.1 * ymax

            ax.set_ylim(ymin, ymax)

        else:
            vmin = np.nanmin(vals)
            vmax = np.nanmax(vals)
            if np.isnan(vmin) or np.isnan(vmax):
                _draw_empty_panel(ax)
                continue

            rango = (vmax - vmin) if vmax > vmin else 1.0

            # Margen proporcional (20%)
            margen_prop = 0.2 * rango

            # Margen mínimo absoluto para evitar compresión en rangos pequeños
            margen_min = 1.0

            margen = max(margen_prop, margen_min)

            ax.set_ylim(vmin - margen, vmax + margen)

        # -----------------------------------
        # Gráfica
        # -----------------------------------
        col = COLOR_VAR.get(var, "#333333")

        if var == "pr":
            ax.bar(fechas, vals, width=0.6, alpha=0.45)
            ax.plot(fechas, vals, "-o", markersize=3, lw=1.0)
        else:
            ax.plot(fechas, vals, "-o", markersize=3, lw=1.0, color=col)

        # -----------------------------------
        # Faltantes -99 como círculos huecos (dibujar después de fijar ylim)
        # -----------------------------------
        if np.any(mask_missing):
            y0 = ax.get_ylim()[0] + 0.05 * (ax.get_ylim()[1] - ax.get_ylim()[0])
            ax.scatter(
                fechas[mask_missing],
                [y0] * mask_missing.sum(),
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
        try:
            manager = plt.get_current_fig_manager()
            mover_figura_a_monitor_opuesto(manager)
        except:
            pass

        plt.show(block=False)

    return fig


# ============================================================
# COMPARACIÓN ORG VS QC
# ============================================================


def plot_comparison_qc(df_org, df_qc, var, periodo, estacion, folder_out):

    # Copias seguras
    df_org = df_org.copy()
    df_qc = df_qc.copy()

    df_org["fecha"] = pd.to_datetime(df_org["fecha"])
    df_qc["fecha"] = pd.to_datetime(df_qc["fecha"])

    # ============================================================
    # Alineación por fecha (outer merge para NO perder información)
    # ============================================================
    dfm = df_org.merge(df_qc, on="fecha", how="outer", suffixes=("_org", "_qc"))
    dfm = dfm.sort_values("fecha").reset_index(drop=True)

    # Convertir valores
    dfm["valor_org"] = pd.to_numeric(dfm["valor_org"], errors="coerce")
    dfm["valor_qc"] = pd.to_numeric(dfm["valor_qc"], errors="coerce")

    fechas = dfm["fecha"].values
    vo = dfm["valor_org"].astype(float).replace(-99, np.nan).values
    vq = dfm["valor_qc"].astype(float).replace(-99, np.nan).values

    # Para detectar modificados
    mask_mod = ~np.isclose(vo, vq, equal_nan=True)

    # Para detectar faltantes
    mask_missing_qc = dfm["valor_qc"].isna() | (dfm["valor_qc"] == -99)

    # ============================================================
    # Gráficos
    # ============================================================
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), dpi=110)

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

    # Marcar modificaciones
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

    # Marcar faltantes
    if np.any(mask_missing_qc):
        # Para colocar faltantes un poco por debajo
        vq_nonan = vq[~np.isnan(vq)]
        if len(vq_nonan) > 0:
            ymin = np.nanmin(vq_nonan)
            ymax = np.nanmax(vq_nonan)
            rng = max(1, ymax - ymin)
            y_offset = ymin - 0.08 * rng
        else:
            y_offset = -1

        ax2.scatter(
            fechas[mask_missing_qc],
            [y_offset] * mask_missing_qc.sum(),
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

    try:
        manager = plt.get_current_fig_manager()
        mover_figura_a_monitor_opuesto(manager)
    except:
        pass

    plt.show(block=False)
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
