#!/usr/bin/env python3
# Parte 1/3 - utilidades, registro, normalización y gráficas
import os
import sys
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
from datetime import datetime, timezone


CHANGES_FNAME = "changes_applied.json"
COMPLETED_FNAME = "completed_series.json"


def path_completed(folder_out):
    return os.path.join(folder_out, COMPLETED_FNAME)


def load_completed(folder_out):
    p = path_completed(folder_out)
    if not os.path.exists(p):
        return {"completadas": []}
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except:
        return {"completadas": []}


def save_completed(folder_out, completed):
    p = path_completed(folder_out)
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(completed, fh, indent=2)


def path_changes(folder_out):
    return os.path.join(folder_out, CHANGES_FNAME)


def load_changes(folder_out):
    p = path_changes(folder_out)
    if not os.path.exists(p):
        return {"swaps": [], "single_changes": []}
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {"swaps": [], "single_changes": []}


def save_changes(folder_out, changes):
    p = path_changes(folder_out)
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(changes, fh, indent=2, ensure_ascii=False)


def register_swap(
    folder_out,
    archivo_1,
    archivo_2,
    estacion,
    fecha_str,
    nuevo_tmax,
    nuevo_tmin,
    nota="",
):
    changes = load_changes(folder_out)
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "archivo_1": archivo_1,
        "archivo_2": archivo_2,
        "estacion": estacion,
        "fecha": fecha_str,
        "valor_nuevo_en_tmax": nuevo_tmax,
        "valor_nuevo_en_tmin": nuevo_tmin,
        "nota": nota,
    }
    changes.setdefault("swaps", []).append(entry)
    save_changes(folder_out, changes)


def register_single_change(folder_out, archivo, fecha_str, original, nuevo, accion):
    changes = load_changes(folder_out)
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "archivo": archivo,
        "fecha": fecha_str,
        "valor_original": original,
        "valor_nuevo": nuevo,
        "accion": accion,
    }
    changes.setdefault("single_changes", []).append(entry)
    save_changes(folder_out, changes)


def apply_pending_changes_to_df(folder_out, base_name, df, fecha_col, val_col):
    """
    Aplica cambios del JSON a df en memoria (single_changes y swaps).
    """
    changes = load_changes(folder_out)
    df[fecha_col] = pd.to_datetime(df[fecha_col])

    for ent in changes.get("single_changes", []):
        if ent.get("archivo") == base_name:
            fecha_obj = pd.to_datetime(ent["fecha"])
            mask = df[fecha_col] == fecha_obj
            if mask.any():
                df.loc[mask, val_col] = ent["valor_nuevo"]

    for ent in changes.get("swaps", []):
        a1 = ent.get("archivo_1")
        a2 = ent.get("archivo_2")
        fecha_obj = pd.to_datetime(ent["fecha"])
        if base_name == a1:
            mask = df[fecha_col] == fecha_obj
            if mask.any():
                df.loc[mask, val_col] = ent.get("valor_nuevo_en_tmax")
        if base_name == a2:
            mask = df[fecha_col] == fecha_obj
            if mask.any():
                df.loc[mask, val_col] = ent.get("valor_nuevo_en_tmin")

    return df


def normalize_missing_values(series):
    s = pd.to_numeric(series, errors="coerce")
    s = s.replace([-99.0, -99.9, -99.00], -99)
    s = s.fillna(-99)
    return s


# ---------- VISUALIZACIÓN ----------


def mostrar_grafica_contexto(
    folder_in,
    estacion,
    variable_base,
    fechas_var_principal,
    valores_var_principal,
    idx,
    ventana=10,
    tick_interval_days=1,
    folder_out=None,
    wait=True,
):
    """
    Muestra figura 2x2 con variable principal y ts/tmax/tmin/pd de la estación.
    Si wait=True: ventana principal bloqueante (se cierra manualmente).
    Si wait=False: ventana no bloqueante (para comparativas).
    """
    import math

    todas = ["ts", "tmax", "tmin", "pd"]
    unidades = {"ts": "°C", "tmax": "°C", "tmin": "°C", "pd": "mm"}
    variable_base = (variable_base or "").lower()
    if variable_base not in todas:
        variable_base = "ts"

    # validación
    if fechas_var_principal is None or len(fechas_var_principal) == 0:
        print(f"⚠️ No hay fechas válidas para graficar en {estacion}.")
        return

    try:
        fecha_anomalia = pd.to_datetime(fechas_var_principal.iloc[idx])
    except Exception:
        fecha_anomalia = pd.to_datetime(fechas_var_principal.iloc[0])
    fecha_inicio = fecha_anomalia - pd.Timedelta(days=ventana)
    fecha_fin = fecha_anomalia + pd.Timedelta(days=ventana)

    # subserie principal
    fechas_main = pd.to_datetime(fechas_var_principal)
    vals_main = np.array(valores_var_principal, dtype=float)
    vals_main[vals_main == -99] = np.nan
    mask_main = (fechas_main >= fecha_inicio) & (fechas_main <= fecha_fin)
    sub_fechas_main = fechas_main[mask_main]
    sub_vals_main = vals_main[mask_main]
    val_anomalia = (
        float(valores_var_principal.iloc[idx])
        if idx < len(valores_var_principal) and valores_var_principal.iloc[idx] != -99
        else np.nan
    )

    archivos = os.listdir(folder_in)
    archivos_lower = [a.lower() for a in archivos]
    var_paths = {
        var: _find_file_for_var(folder_in, var, estacion, folder_out) for var in todas
    }

    def cargar_y_filtrar(path):
        if not path or not os.path.exists(path):
            return None, None
        try:
            dff = pd.read_csv(path, sep=None, engine="python")
        except Exception:
            return None, None
        dff.columns = [c.strip() for c in dff.columns]
        if len(dff.columns) < 2:
            return None, None
        dff[dff.columns[0]] = pd.to_datetime(
            dff[dff.columns[0]].astype(str), format="%Y%m%d", errors="coerce"
        )
        dff = dff.dropna(subset=[dff.columns[0]])
        dff[dff.columns[1]] = normalize_missing_values(dff[dff.columns[1]])
        fechas = dff[dff.columns[0]]
        valores = dff[dff.columns[1]]
        mask = (fechas >= fecha_inicio) & (fechas <= fecha_fin)
        fechas_sub = fechas[mask]
        valores_sub = valores[mask]
        if len(fechas_sub) == 0:
            return None, None
        return fechas_sub.reset_index(drop=True), valores_sub.reset_index(drop=True)

    fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True)
    fig.suptitle(
        f"Contexto estación {estacion} — {fecha_anomalia.strftime('%Y-%m-%d')} ({variable_base})",
        fontsize=12,
    )
    axes = axes.flatten()

    locator = mdates.DayLocator(interval=max(1, int(tick_interval_days)))
    formatter = mdates.DateFormatter("%Y-%m-%d")

    def plot_variable(ax, fechas, valores, titulo, unidad, marcar_anomalia=False):
        ax.clear()
        ax.set_xlim(fecha_inicio, fecha_fin)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
        ax.tick_params(axis="x", rotation=45, labelsize=8)

        if fechas is None or valores is None or len(fechas) == 0:
            ax.set_title(f"{titulo} ({unidad}) — Sin datos")
            ax.text(
                0.5,
                0.5,
                "Sin datos para el periodo",
                ha="center",
                va="center",
                transform=ax.transAxes,
                fontsize=10,
                alpha=0.7,
            )
            ax.grid(True, linestyle="--", alpha=0.4)
            return

        fechas = pd.to_datetime(fechas)
        vals = np.array(valores, dtype=float)
        vals[vals == -99] = np.nan

        if np.all(np.isnan(vals)):
            ymin, ymax = 0, 1
        else:
            vmin, vmax = np.nanmin(vals), np.nanmax(vals)
            rango = vmax - vmin if vmax > vmin else 1
            margen = 0.1 * rango
            ymin, ymax = vmin - margen, vmax + margen
        ax.set_ylim(ymin, ymax)

        ax.plot(
            fechas,
            vals,
            "-o",
            markersize=4,
            linewidth=0.9,
            color="tab:blue",
            label="Serie",
        )
        ax.axvline(fecha_anomalia, color="gray", linestyle="--", linewidth=1)

        for i, (f, v) in enumerate(zip(fechas, vals)):
            if np.isnan(v):
                continue
            offset = 0.03 * (ymax - ymin) * ((-1) ** i)
            ax.text(
                f,
                v + offset,
                f"{v:.1f}",
                fontsize=7,
                ha="center",
                va="bottom" if offset > 0 else "top",
                color="black",
            )

        if marcar_anomalia and not np.isnan(val_anomalia):
            # intentar marcar valor existente en la subserie (si coincide)
            mask_exact = fechas == fecha_anomalia
            y_val = val_anomalia
            if mask_exact.any():
                try:
                    y_val = np.array(vals[mask_exact])[0]
                except Exception:
                    pass
            if not np.isnan(y_val):
                ax.scatter(
                    [fecha_anomalia],
                    [y_val],
                    color="red",
                    s=80,
                    edgecolor="black",
                    zorder=5,
                    label=f"Anómalo: {val_anomalia:.1f} {unidad}",
                )

        ax.set_title(f"{titulo} ({unidad})")
        ax.grid(True, linestyle="--", alpha=0.5)
        ax.legend(fontsize=8)

    # Panel principal
    plot_variable(
        axes[0],
        sub_fechas_main,
        sub_vals_main,
        variable_base,
        unidades.get(variable_base, ""),
        marcar_anomalia=True,
    )

    pos = 1
    for var in [v for v in todas if v != variable_base]:
        fechas_sub, vals_sub = cargar_y_filtrar(var_paths[var])
        plot_variable(axes[pos], fechas_sub, vals_sub, var, unidades.get(var, ""))
        pos += 1

    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # guardar figura principal si corresponde
    if folder_out and wait:
        subdir = os.path.join(folder_out, "figuras-anomalias")
        os.makedirs(subdir, exist_ok=True)
        nombre_fig = (
            f"{variable_base}_{estacion}_{fecha_anomalia.strftime('%Y-%m-%d')}.png"
        )
        ruta_fig = os.path.join(subdir, nombre_fig)
        try:
            plt.savefig(ruta_fig, dpi=200, bbox_inches="tight")
            print(f"🖼️  Figura principal guardada en: {ruta_fig}")
        except Exception as e:
            print(f"⚠️ No se pudo guardar la figura principal: {e}")

    plt.show(block=False)
    try:
        mng = plt.get_current_fig_manager()
        if hasattr(mng, "window"):
            # Posicionar y traer al frente
            if wait:
                mng.window.wm_geometry("+100+100")  # ventana principal
            else:
                mng.window.wm_geometry("+900+100")  # comparativas

            # Hacerla topmost momentáneamente (solo Tk)
            mng.window.attributes("-topmost", 1)
            mng.window.update()
            mng.window.attributes("-topmost", 0)
    except Exception:
        pass

    if wait:
        print("🔍 Visualice la figura principal; ciérrela para continuar.")


def graficar_comparativa(
    fechas,
    original,
    corregido,
    ruta_salida,
    titulo="Comparativa",
    no_outliers=False,
):
    """
    Genera una figura 2x1:
      - Arriba: serie original.
      - Abajo: serie corregida después del control de calidad.
    Los valores modificados se marcan con círculos rojos.
    Los valores con -99 (datos faltantes) no se grafican.
    """
    # --- Preparar datos ---
    fechas = pd.to_datetime(fechas)
    original = np.array(original, dtype=float)
    corregido = np.array(corregido, dtype=float)

    # Reemplazar -99 por NaN para ocultarlos
    original = np.where(original == -99, np.nan, original)
    corregido = np.where(corregido == -99, np.nan, corregido)

    # --- Crear figura con 2 subplots verticales ---
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), dpi=150, sharex=True)
    fig.suptitle(titulo, fontsize=13, fontweight="bold")

    # --- PANEL 1: Serie original ---
    axes[0].plot(fechas, original, "-", lw=0.5, color="gray", label="Original")
    axes[0].set_title("Serie original")
    axes[0].set_ylabel("Valor")
    axes[0].grid(alpha=0.4)
    axes[0].legend()

    # --- PANEL 2: Serie corregida ---
    axes[1].plot(fechas, corregido, "-", lw=0.5, color="blue", label="Corregida")

    # Marcar puntos modificados
    mask_mod = ~np.isclose(original, corregido, equal_nan=True)
    if np.any(mask_mod):
        axes[1].scatter(
            fechas[mask_mod],
            corregido[mask_mod],
            s=80,
            facecolors="none",
            edgecolors="red",
            linewidths=1.3,
            label="Modificado",
            zorder=5,
        )

    # --- Mensaje si no hay outliers ---
    if no_outliers:
        axes[1].text(
            0.5,
            0.5,
            "No se detectaron outliers",
            transform=axes[1].transAxes,
            ha="center",
            va="center",
            fontsize=13,
            fontweight="bold",
            color="darkgreen",
            bbox=dict(
                facecolor="white",
                edgecolor="darkgreen",
                boxstyle="round,pad=0.5",
                alpha=0.9,
            ),
        )

    # --- Formato general ---
    axes[1].set_title("Serie corregida")
    axes[1].set_xlabel("Fecha")
    axes[1].set_ylabel("Valor")
    axes[1].grid(alpha=0.4)
    axes[1].legend()

    # Mejor ajuste de ejes y formato de fechas
    fig.autofmt_xdate()
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # --- Guardar figura ---
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    plt.savefig(ruta_salida, dpi=200, bbox_inches="tight")
    plt.close(fig)


# Parte 2/3 - verificación y corrección termodinámica


# ---------------------------------------------------------------------
# 🧭 FUNCIONES AUXILIARES PARA CONTROL TÉRMICO
# ---------------------------------------------------------------------


def _find_file_for_var(folder_in, var, estacion, folder_out=None):
    """
    Busca archivo para una variable (ts, tmax, tmin).
    Regla:
      1) Si existe *_QC.csv en folder_out → usar ese
      2) Si existe versión NO-QC en folder_out → usar ese
      3) Si no existe, usar archivo original (folder_in)
    """

    var = var.lower()
    estacion = estacion.lower()

    # 1️⃣ Buscar primero versiones QC en carpeta de salida
    if folder_out:
        for fname in os.listdir(folder_out):
            f = fname.lower()
            if f.startswith(f"{var}_") and f.endswith(f"_{estacion}_qc.csv"):
                return os.path.join(folder_out, fname)

        # 2️⃣ Buscar versiones NO-QC en carpeta de salida
        for fname in os.listdir(folder_out):
            f = fname.lower()
            if f.startswith(f"{var}_") and f.endswith(f"_{estacion}.csv"):
                return os.path.join(folder_out, fname)

    # 3️⃣ Buscar original en carpeta de entrada
    for fname in os.listdir(folder_in):
        f = fname.lower()
        if f.startswith(f"{var}_") and f.endswith(f"_{estacion}.csv"):
            return os.path.join(folder_in, fname)

    return None


def _save_df_as_qc(df, ruta_original, folder_out):
    """
    Guarda el DataFrame en la carpeta de salida con sufijo _QC.csv.
    """
    base_name = os.path.basename(ruta_original)
    clean = base_name.replace("_QC", "").replace(".csv", "")
    salida_name = f"{clean}_QC.csv"
    ruta_salida = os.path.join(folder_out, salida_name)
    df_out = df.copy()
    if isinstance(df_out.iloc[0, 0], pd.Timestamp):
        df_out[df_out.columns[0]] = df_out[df_out.columns[0]].dt.strftime("%Y%m%d")
    df_out.to_csv(ruta_salida, index=False)
    return ruta_salida


# ---------------------------------------------------------------------
# 🌡️ DETECCIÓN DE INCONSISTENCIAS TERMODINÁMICAS
# ---------------------------------------------------------------------


def verificar_inconsistencias_termicas(folder_in, folder_out, estacion):
    """
    Recorre los registros de tmin, ts y tmax para la estación dada
    y detecta días donde no se cumpla tmin < ts < tmax.

    Devuelve lista de dicts con:
        {'fecha': ..., 'tmin': ..., 'ts': ..., 'tmax': ..., 'inconsistente': True}
    """
    rutas = {
        v: _find_file_for_var(folder_in, v, estacion, folder_out)
        for v in ["tmin", "ts", "tmax"]
    }
    dfs = {}
    for v, ruta in rutas.items():
        if not ruta or not os.path.exists(ruta):
            continue
        df = pd.read_csv(ruta, sep=None, engine="python")
        df.columns = [c.strip() for c in df.columns]
        df[df.columns[0]] = pd.to_datetime(
            df[df.columns[0]].astype(str), format="%Y%m%d", errors="coerce"
        )
        df[df.columns[1]] = normalize_missing_values(df[df.columns[1]])
        dfs[v] = df.rename(columns={df.columns[0]: "fecha", df.columns[1]: v})

    # fusionar por fecha
    if len(dfs) < 2:
        return []

    df_merged = None
    for v in dfs:
        df_merged = (
            dfs[v]
            if df_merged is None
            else pd.merge(df_merged, dfs[v], on="fecha", how="outer")
        )

    df_merged = df_merged.sort_values("fecha").reset_index(drop=True)
    inconsistencias = []

    for _, row in df_merged.iterrows():
        tmin, ts, tmax = row.get("tmin", -99), row.get("ts", -99), row.get("tmax", -99)
        if -99 in (tmin, ts, tmax) or pd.isna(tmin) or pd.isna(ts) or pd.isna(tmax):
            continue
        if not (tmin < ts < tmax):
            inconsistencias.append(
                {
                    "fecha": row["fecha"],
                    "tmin": float(tmin),
                    "ts": float(ts),
                    "tmax": float(tmax),
                    "inconsistente": True,
                }
            )
    return inconsistencias


# ---------------------------------------------------------------------
# 🧠 APLICACIÓN INTERACTIVA DE CORRECCIONES TÉRMICAS
# ---------------------------------------------------------------------


def aplicar_correccion_termica_interactiva(
    folder_in, folder_out, estacion, incons, logs_local
):
    """
    Interfaz interactiva con lógica inteligente de control térmico:
      - Detecta tipo de inconsistencia (tmax < tmin, ts fuera de rango, etc.)
      - Propone acción recomendada con confirmación
      - Permite edición manual, sustitución por -99 o mantener sin cambios
      - Registra la acción en JSON y log local
    """
    fecha_obj = pd.to_datetime(incons["fecha"])
    fecha_str = fecha_obj.strftime("%Y-%m-%d")

    # Cargar valores actuales
    tmin = incons.get("tmin")
    ts = incons.get("ts")
    tmax = incons.get("tmax")

    # Determinar tipo de inconsistencia
    tipo = None
    if tmin is not None and tmax is not None and tmax < tmin:
        tipo = "tmax<tmin"
    elif ts is not None and tmax is not None and ts > tmax:
        tipo = "ts>tmax"
    elif ts is not None and tmin is not None and ts < tmin:
        tipo = "ts<tmin"
    elif tmin == tmax and tmin is not None:
        tipo = "tmin==tmax"
    else:
        tipo = "indefinido"

    print(f"\n⚠️ Inconsistencia térmica detectada en estación {estacion} ({fecha_str})")
    print(f"   tmin={tmin}, ts={ts}, tmax={tmax}")
    print(f"👉 Tipo: {tipo}")

    # Mostrar recomendación según el tipo
    if tipo == "tmax<tmin":
        print("   Posible inversión: se recomienda intercambiar tmax ↔ tmin.")
    elif tipo == "ts>tmax":
        print("   ts supera la temperatura máxima: revisar ts o tmax.")
    elif tipo == "ts<tmin":
        print("   ts menor que la mínima: revisar ts o tmin.")
    elif tipo == "tmin==tmax":
        print("   tmin y tmax iguales: posible error de digitación.")
    else:
        print("   Inconsistencia indefinida, revisión manual sugerida.")

    # Mostrar contexto gráfico
    try:
        # Buscar archivo real de la variable ts para esta estación
        ruta_ts = _find_file_for_var(folder_in, "ts", estacion, folder_out)
        if ruta_ts and os.path.exists(ruta_ts):
            df_ts = pd.read_csv(ruta_ts, sep=None, engine="python")
            df_ts.columns = [c.strip() for c in df_ts.columns]
            df_ts[df_ts.columns[0]] = pd.to_datetime(
                df_ts[df_ts.columns[0]].astype(str), format="%Y%m%d", errors="coerce"
            )
            df_ts[df_ts.columns[1]] = normalize_missing_values(df_ts[df_ts.columns[1]])

            # Buscar índice del valor correspondiente a la fecha
            idx_anomalia = (df_ts[df_ts.columns[0]] == fecha_obj).idxmax()

            mostrar_grafica_contexto(
                folder_in,
                estacion,
                "ts",
                df_ts[df_ts.columns[0]],
                df_ts[df_ts.columns[1]],
                idx_anomalia,
                ventana=7,
                folder_out=folder_out,
                wait=True,
            )
        else:
            print("⚠️ No se encontró archivo de ts para mostrar contexto.")
    except Exception as e:
        print(f"⚠️ No se pudo mostrar el contexto: {e}")

    # --- Menú de acción con validación ---
    while True:
        print("\nSeleccione acción:")
        if tipo == "tmax<tmin":
            print("  (i) Intercambiar tmax ↔ tmin")
        if tipo in ("ts>tmax", "ts<tmin"):
            print("  (t) Sustituir solo ts por -99")
            print("  (x) Sustituir ts y la variable afectada por -99")
        print("  (e) Editar manualmente los 3 valores")
        print("  (s) Sustituir los 3 valores por -99")
        print("  (m) Mantener sin cambios")
        print("  (r) Reordenar automáticamente (tmin < ts < tmax)")

        resp = input("Elija acción: ").strip().lower()

        # Definir opciones válidas dinámicamente según el tipo
        opciones_validas = {"e", "s", "m", "r"}
        if tipo == "tmax<tmin":
            opciones_validas.add("i")
        if tipo in ("ts>tmax", "ts<tmin"):
            opciones_validas.update({"t", "x"})

        if resp in opciones_validas:
            break
        else:
            print(
                f"⚠️ Opción '{resp}' no válida. Ingrese una de: {', '.join(sorted(opciones_validas))}."
            )

    # Cargar dataframes de cada variable
    rutas = {
        v: _find_file_for_var(folder_in, v, estacion, folder_out)
        for v in ["tmin", "ts", "tmax"]
    }
    dfs = {}
    for v, ruta in rutas.items():
        if ruta and os.path.exists(ruta):
            dft = pd.read_csv(ruta, sep=None, engine="python")
            dft.columns = [c.strip() for c in dft.columns]
            dft[dft.columns[0]] = pd.to_datetime(
                dft[dft.columns[0]].astype(str), format="%Y%m%d", errors="coerce"
            )
            dft[dft.columns[1]] = normalize_missing_values(dft[dft.columns[1]])
            dfs[v] = dft
        else:
            dfs[v] = None

    def update_val(var, nuevo_val, motivo):
        d = dfs[var]
        ruta = rutas[var]
        if d is None or ruta is None:
            return
        mask = d[d.columns[0]] == fecha_obj
        # No modificar si esta serie está completada
        completed = load_completed(folder_out)
        base_name = os.path.basename(ruta)

        if base_name in completed["completadas"]:
            print(f"⛔ {base_name} está marcado como COMPLETADO. No se modificará.")
            return

        if mask.any():
            val_old = float(d.loc[mask, d.columns[1]].values[0])
            d.loc[mask, d.columns[1]] = nuevo_val
            _save_df_as_qc(d, ruta, folder_out)

    # Ejecutar acción
    accion = ""
    if resp == "m":
        print("✅ Manteniendo sin cambios.")
        accion = "mantener"
    elif resp == "i" and tipo == "tmax<tmin":
        print("🔁 Intercambiando tmax y tmin...")
        update_val("tmin", tmax, "ajuste_termico_inversion")
        update_val("tmax", tmin, "ajuste_termico_inversion")
        accion = "intercambio_tmax_tmin"
    elif resp == "t" and tipo in ("ts>tmax", "ts<tmin"):
        print("❄️ Sustituyendo solo ts por -99.")
        update_val("ts", -99.0, "ajuste_termico_ts_fuera_rango")
        accion = "sustituir_ts"
    elif resp == "x" and tipo in ("ts>tmax", "ts<tmin"):
        print("🔥 Sustituyendo ts y variable afectada por -99.")
        update_val("ts", -99.0, "ajuste_termico_ts_fuera_rango")
        if tipo == "ts>tmax":
            update_val("tmax", -99.0, "ajuste_termico_ts_fuera_rango")
        else:
            update_val("tmin", -99.0, "ajuste_termico_ts_fuera_rango")
        accion = "sustituir_ts_y_otro"
    elif resp == "e":
        print("✏️ Edición manual de valores (enter para mantener):")
        nuevos = {}
        for v in ["tmin", "ts", "tmax"]:
            val = input(f"  Nuevo {v}: ").strip()
            if val == "":
                nuevos[v] = incons.get(v)
            else:
                try:
                    nuevos[v] = float(val)
                except ValueError:
                    nuevos[v] = incons.get(v)
        for v in nuevos:
            if nuevos[v] is not None:
                update_val(v, nuevos[v], "ajuste_termico_manual")
        accion = "ajuste_manual"
    elif resp == "s":
        print("🧹 Sustituyendo los tres valores por -99.")
        for v in ["tmin", "ts", "tmax"]:
            update_val(v, -99.0, "ajuste_termico_sustituir_todo")
        accion = "sustituir_todo"
    elif resp == "r":
        print(
            "🔧 Reordenando automáticamente las tres temperaturas (tmin < ts < tmax)..."
        )
        valores = {"tmin": tmin, "ts": ts, "tmax": tmax}
        # ignorar los -99 para evitar falsos reordenamientos
        validos = {k: v for k, v in valores.items() if v is not None and v != -99}
        if len(validos) >= 2:
            ordenados = dict(zip(["tmin", "ts", "tmax"], sorted(validos.values())))
            for v in ordenados:
                if ordenados[v] != valores[v]:
                    update_val(v, ordenados[v], "reordenamiento_automatico")
            accion = "reordenar_termicamente"
            print("✅ Reordenamiento aplicado.")
        else:
            print("⚠️ No hay suficientes valores válidos para reordenar.")

    # Registrar en log local
    logs_local.append(
        {
            "archivo": f"{estacion}",
            "fecha": fecha_str,
            "tipo_inconsistencia": tipo,
            "accion_termica": accion,
            "valores_previos": {"tmin": tmin, "ts": ts, "tmax": tmax},
            "valores_nuevos": {
                "tmin": (
                    dfs["tmin"]
                    .loc[
                        dfs["tmin"][dfs["tmin"].columns[0]] == fecha_obj,
                        dfs["tmin"].columns[1],
                    ]
                    .values[0]
                    if dfs["tmin"] is not None
                    else None
                ),
                "ts": (
                    dfs["ts"]
                    .loc[
                        dfs["ts"][dfs["ts"].columns[0]] == fecha_obj,
                        dfs["ts"].columns[1],
                    ]
                    .values[0]
                    if dfs["ts"] is not None
                    else None
                ),
                "tmax": (
                    dfs["tmax"]
                    .loc[
                        dfs["tmax"][dfs["tmax"].columns[0]] == fecha_obj,
                        dfs["tmax"].columns[1],
                    ]
                    .values[0]
                    if dfs["tmax"] is not None
                    else None
                ),
            },
        }
    )

    print(f"✅ Acción '{accion}' aplicada a {estacion} en {fecha_str}.")
    plt.close("all")

    return True


# Parte 3/3 - integración al flujo interactivo, main_batch y entrypoint

# ---------------------------------------------------------------------
# 🧮 PROCESAMIENTO INTERACTIVO DE ARCHIVOS (con control térmico)
# ---------------------------------------------------------------------


def procesar_archivo_interactivo(
    ruta_input,
    ruta_output,
    lower_percentile,
    upper_percentile,
    k_iqr,
    ventana=7,
    aplicar_previos=False,
):
    """
    Procesa un archivo CSV (FECHA, VALOR) aplicando control de calidad físico y estadístico.

    Flujo de trabajo:
    -----------------
    1️⃣ Verificación termodinámica (tmin < ts < tmax).
    2️⃣ Detección estadística de outliers (percentiles/IQR).
    3️⃣ Revisión manual (interactiva) o automática según elección del usuario.
    4️⃣ Registro de cambios en JSON y CSV.
    5️⃣ Generación de gráficas comparativas y de contexto.
    """
    # --- lectura y validación del archivo ---
    # Determinar variable y estación
    nombre_archivo = os.path.basename(ruta_input)
    variable_principal = nombre_archivo.split("_")[0].lower()
    estacion = nombre_archivo.split("_")[-1].replace(".csv", "")

    # Buscar la versión más reciente (QC si existe)
    ruta_qc_actual = _find_file_for_var(
        os.path.dirname(ruta_input), variable_principal, estacion, ruta_output
    )

    ruta_a_usar = ruta_qc_actual if ruta_qc_actual else ruta_input

    df = pd.read_csv(ruta_a_usar, sep=None, engine="python")

    df.columns = [c.strip() for c in df.columns]
    if len(df.columns) < 2:
        raise ValueError(f"El archivo {ruta_input} no tiene al menos 2 columnas.")

    fecha_col = df.columns[0]
    val_col = df.columns[1]

    # convertir columna FECHA al formato datetime
    df[fecha_col] = pd.to_datetime(
        df[fecha_col].astype(str), format="%Y%m%d", errors="coerce"
    )
    if df[fecha_col].isna().any():
        print(
            f"⚠️ Hay filas con FECHA inválida en {os.path.basename(ruta_input)}; serán eliminadas."
        )
        df = df.dropna(subset=[fecha_col]).reset_index(drop=True)

    # normalizar valores faltantes (-99)
    df[val_col] = normalize_missing_values(df[val_col])

    # -----------------------------------------------------------------
    # ✔ APLICAR CAMBIOS PREVIOS DEL JSON SI SE INDICÓ
    # -----------------------------------------------------------------
    if aplicar_previos:
        base_name = os.path.basename(ruta_input)
        print(f"🔧 Aplicando cambios previos del JSON a {base_name}…")
        df = apply_pending_changes_to_df(ruta_output, base_name, df, fecha_col, val_col)
        print("✔ Cambios previos aplicados.")

    # conservar copia original para graficar comparativo luego
    df_original = df.copy()

    # Esta pregunta adicional solo se hace si aplicar_previos==False
    if not aplicar_previos:
        cambios_previos = load_changes(ruta_output)
        if cambios_previos["single_changes"] or cambios_previos["swaps"]:
            aplicar2 = (
                input(
                    "Existen cambios previos registrados en el JSON. ¿Desea aplicarlos a esta serie antes de revisar? (s/n): "
                )
                .strip()
                .lower()
            )
            if aplicar2 in ("s", "y"):
                df = apply_pending_changes_to_df(
                    ruta_output, os.path.basename(ruta_input), df, fecha_col, val_col
                )

    # determinar estación y variable
    nombre_archivo = os.path.basename(ruta_input)
    estacion = nombre_archivo.split("_")[-1].replace(".csv", "")
    variable_principal = nombre_archivo.split("_")[0].lower()

    logs_local = []

    # -----------------------------------------------------------------
    # 🌡️ PRIMERA ETAPA: VERIFICACIÓN TERMODINÁMICA
    # -----------------------------------------------------------------
    if variable_principal in ("tmin", "ts", "tmax"):
        inconsistencias = verificar_inconsistencias_termicas(
            os.path.dirname(ruta_input), ruta_output, estacion
        )

        if inconsistencias:
            print(
                f"\n🌡️ Se detectaron {len(inconsistencias)} posibles inconsistencias térmicas en {estacion}."
            )
            revisar = (
                input("¿Desea revisar estas inconsistencias ahora? (s/n): ")
                .strip()
                .lower()
            )
            if revisar in ("s", "y"):
                for incons in inconsistencias:
                    aplicar_correccion_termica_interactiva(
                        os.path.dirname(ruta_input),
                        ruta_output,
                        estacion,
                        incons,
                        logs_local,
                    )
            else:
                print("⏭️ Inconsistencias térmicas omitidas en esta ejecución.")
        else:
            print(f"✅ No se detectaron inconsistencias térmicas en {estacion}.")

    # -----------------------------------------------------------------
    # 📈 SEGUNDA ETAPA: DETECCIÓN DE OUTLIERS ESTADÍSTICOS
    # -----------------------------------------------------------------
    series_no_missing = df.loc[df[val_col] != -99, val_col]
    if len(series_no_missing) < 5:
        print(
            f"⚠️ Pocos datos válidos en {os.path.basename(ruta_input)} (n={len(series_no_missing)}). Se omitirá este archivo."
        )
        return logs_local

    P_low = series_no_missing.quantile(lower_percentile)
    P_high = series_no_missing.quantile(upper_percentile)
    IQR = P_high - P_low
    limite_inf = P_low - k_iqr * IQR
    limite_sup = P_high + k_iqr * IQR

    # --- detección de anomalías ---
    anomalos_idx = []
    for idx, val in df[val_col].items():
        if pd.isna(val) or val == -99:
            continue
        if (val < limite_inf) or (val > limite_sup):
            anomalos_idx.append((idx, val))

    print(
        f"\nArchivo: {os.path.basename(ruta_input)} → detectadas {len(anomalos_idx)} anomalías estadísticas (k={k_iqr})."
    )

    # -----------------------------------------------------------------
    # 🚫 CASO: SIN OUTLIERS ESTADÍSTICOS
    # -----------------------------------------------------------------
    if len(anomalos_idx) == 0:
        print(
            "✅ No se detectaron outliers estadísticos — se copiará el archivo sin modificaciones."
        )
        base_name = os.path.basename(ruta_input)

        # aplicar cambios previos si existiesen en JSON
        df = apply_pending_changes_to_df(ruta_output, base_name, df, fecha_col, val_col)

        salida_name = base_name.replace(".csv", "_QC.csv")
        ruta_salida = os.path.join(ruta_output, salida_name)

        # guardar copia idéntica
        df_to_save = df.copy()
        df_to_save[fecha_col] = df_to_save[fecha_col].dt.strftime("%Y%m%d")
        df_to_save.to_csv(ruta_salida, index=False)

        # generar gráfica comparativa
        png_name = base_name.replace(".csv", "_QC_compare.png")
        ruta_png = os.path.join(ruta_output, png_name)
        graficar_comparativa(
            df_original[fecha_col],
            df_original[val_col].values,
            df[val_col].values,
            ruta_png,
            titulo=base_name,
            no_outliers=True,
        )

        print(f"Archivo copiado en: {ruta_salida}")
        print(f"Gráfica guardada en: {ruta_png}")

        logs_local.append(
            {
                "archivo": base_name,
                "accion": "sin_outliers",
                "fecha_proceso": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        return logs_local

    # -----------------------------------------------------------------
    # ✋ CASO: HAY OUTLIERS DETECTADOS
    # -----------------------------------------------------------------
    respuesta_global = (
        input("¿Desea revisar interactivamente las anomalías estadísticas? (s/n): ")
        .strip()
        .lower()
    )
    if respuesta_global not in ("s", "y"):
        print("⏭️ Revisión de outliers omitida.")
        logs_local.append(
            {
                "archivo": os.path.basename(ruta_input),
                "accion": "omitido_por_usuario",
                "fecha_proceso": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        return logs_local

    # --- ciclo interactivo de revisión ---
    for idx, val in anomalos_idx:
        print("\n-----------------------------------------")
        fecha_str = df.at[idx, fecha_col].strftime("%Y-%m-%d")
        print(f"Fecha: {fecha_str}  |  Valor detectado: {val}  (fila {idx})")

        # mostrar gráfica contextual
        mostrar_grafica_contexto(
            os.path.dirname(ruta_input),
            estacion,
            variable_principal,
            df[fecha_col],
            df[val_col],
            idx,
            ventana=ventana,
            folder_out=ruta_output,
        )

        # -----------------------------------------------------------
        # 🚀 Comparaciones múltiples con otras estaciones
        # -----------------------------------------------------------
        break_para_accion = False
        while True:
            comparar_otro = (
                input("¿Desea comparar con otra estación para el mismo día? (s/n): ")
                .strip()
                .lower()
            )
            if comparar_otro not in ("s", "y"):
                # si el usuario elige no comparar, salir del bucle y pasar a la acción
                break_para_accion = True
                break

            estacion_comp = input(
                "Ingrese el código de la otra estación (ej. S-12): "
            ).strip()
            if not estacion_comp:
                print("⚠️ Código vacío.")
                continue

            try:
                fecha_obj = df.at[idx, fecha_col]
                folder_in = os.path.dirname(ruta_input)
                archivo_aux = _find_file_for_var(
                    folder_in, variable_principal, estacion_comp, folder_out
                )
                if not archivo_aux:
                    print(
                        f"⚠️ No se encontró archivo para {variable_principal.upper()} en {estacion_comp}."
                    )
                    continue

                df_aux = pd.read_csv(archivo_aux, sep=None, engine="python")
                df_aux.columns = [c.strip() for c in df_aux.columns]
                df_aux[df_aux.columns[0]] = pd.to_datetime(
                    df_aux[df_aux.columns[0]].astype(str),
                    format="%Y%m%d",
                    errors="coerce",
                )
                df_aux[df_aux.columns[1]] = normalize_missing_values(
                    df_aux[df_aux.columns[1]]
                )

                idx_cercano = int(
                    (df_aux[df_aux.columns[0]] - fecha_obj).abs().idxmin()
                )
                mostrar_grafica_contexto(
                    folder_in,
                    estacion_comp,
                    variable_principal,
                    df_aux[df_aux.columns[0]],
                    df_aux[df_aux.columns[1]],
                    idx_cercano,
                    ventana=ventana,
                    folder_out=ruta_output,
                    wait=False,
                )
                print(f"🟢 Comparativa abierta para estación {estacion_comp}.")
            except Exception as e:
                print(f"⚠️ Error al cargar estación {estacion_comp}: {e}")

        # si se eligió no comparar, continuar inmediatamente con la acción
        if break_para_accion:
            print("➡️  Continuando con la revisión de la anomalía actual...")

        # pedir acción
        accion = None
        while True:
            resp = (
                input(
                    "Acción - Sustituir por -99 (s), Mantener (m), Nuevo valor (n), Intercambiar con otra variable (i): "
                )
                .strip()
                .lower()
            )
            if resp in ("s", "y"):
                nuevo_val, accion = -99.0, "-99"
                break
            if resp == "m":
                nuevo_val, accion = df.at[idx, val_col], "mantener"
                break
            if resp == "n":
                try:
                    nuevo_val = float(input("Ingrese nuevo valor: "))
                    accion = str(nuevo_val)
                    break
                except ValueError:
                    print("Valor inválido.")
            if resp == "i" and variable_principal in ("tmax", "tmin"):
                pareja = "tmin" if variable_principal == "tmax" else "tmax"
                archivo_pareja = _find_file_for_var(
                    os.path.dirname(ruta_input), pareja, estacion, folder_out
                )
                if not archivo_pareja:
                    print(f"⚠️ No se encontró el archivo pareja: {pareja}")
                    continue
                df_p = pd.read_csv(archivo_pareja, sep=None, engine="python")
                df_p.columns = [c.strip() for c in df_p.columns]
                df_p[df_p.columns[0]] = pd.to_datetime(
                    df_p[df_p.columns[0]].astype(str), format="%Y%m%d", errors="coerce"
                )
                fecha_obj = df.at[idx, fecha_col]
                mask_p = df_p[df_p.columns[0]] == fecha_obj
                if not mask_p.any():
                    print(
                        f"⚠️ Fecha {fecha_obj.strftime('%Y-%m-%d')} no encontrada en {pareja}.csv"
                    )
                    continue
                val_original = df.at[idx, val_col]
                val_p_original = df_p.loc[mask_p, df_p.columns[1]].values[0]
                df.at[idx, val_col], df_p.loc[mask_p, df_p.columns[1]] = (
                    val_p_original,
                    val_original,
                )
                nuevo_val, accion = val_p_original, "intercambio"
                _save_df_as_qc(df_p, archivo_pareja, ruta_output)
                register_swap(
                    ruta_output,
                    os.path.basename(ruta_input),
                    os.path.basename(archivo_pareja),
                    estacion,
                    fecha_obj.strftime("%Y-%m-%d"),
                    float(df.at[idx, val_col]),
                    float(df_p.loc[mask_p, df_p.columns[1]].values[0]),
                    nota="intercambio_interactivo_usuario",
                )
                print(
                    f"🔁 Intercambio {variable_principal} ↔ {pareja} aplicado para {fecha_obj.strftime('%Y-%m-%d')}"
                )
                break
            print("Opción inválida.")
        plt.close("all")

        # registrar y aplicar
        df.at[idx, val_col] = nuevo_val
        logs_local.append(
            {
                "archivo": os.path.basename(ruta_input),
                "fecha": fecha_str,
                "valor_original": val,
                "valor_nuevo": nuevo_val,
                "accion": accion,
                "percentil_inferior": lower_percentile,
                "percentil_superior": upper_percentile,
                "k_iqr": k_iqr,
            }
        )

    # --- guardar archivo corregido ---
    base_name = os.path.basename(ruta_input)
    salida_name = base_name.replace(".csv", "_QC.csv")
    ruta_salida = os.path.join(ruta_output, salida_name)
    df_to_save = df.copy()
    df_to_save[fecha_col] = df_to_save[fecha_col].dt.strftime("%Y%m%d")
    df_to_save.to_csv(ruta_salida, index=False)

    # --- graficar comparativa ---
    png_name = base_name.replace(".csv", "_QC_compare.png")
    ruta_png = os.path.join(ruta_output, png_name)
    graficar_comparativa(
        df_original[fecha_col],
        df_original[val_col].values,
        df[val_col].values,
        ruta_png,
        titulo=base_name,
        no_outliers=False,
    )

    print(f"Archivo corregido guardado en: {ruta_salida}")
    print(f"Comparativa guardada en: {ruta_png}")

    logs_local.append(
        {
            "archivo": os.path.basename(ruta_input),
            "accion": "procesado_interactivamente",
            "fecha_proceso": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    completed = load_completed(ruta_output)
    completed["completadas"].append(base_name)
    save_completed(ruta_output, completed)

    return logs_local


# ---------------------------------------------------------------------
# 🚀 PROCESAMIENTO POR LOTE DE TODA LA CARPETA (con trazabilidad térmica)
# ---------------------------------------------------------------------


def main_batch(folder_in, folder_out):
    """
    Procesa todos los archivos ts_*, tmax_* y tmin_* en una carpeta aplicando:

      ✅ Verificación termodinámica (tmin < ts < tmax)
      ✅ Control de calidad estadístico (percentiles/IQR)
      ✅ Revisión interactiva
      ✅ Registro de cambios en JSON y CSV
      ✅ Reanudación segura en caso de interrupción
      ✅ Omisión de archivos ya procesados
    """
    os.makedirs(folder_out, exist_ok=True)

    # solicitar parámetros de percentiles e IQR
    while True:
        try:
            lp = float(input("Ingrese percentil inferior entre 0 y 1 (ej. 0.10): "))
            up = float(input("Ingrese percentil superior entre 0 y 1 (ej. 0.90): "))
            k = float(input("Ingrese factor multiplicador del IQR (ej. 1.5): "))
            if not (0 <= lp < up <= 1):
                print("Percentiles inválidos — asegúrese de que 0 <= lp < up <= 1.")
                continue
            break
        except ValueError:
            print("Entrada inválida. Intente nuevamente.")

    # recopilar archivos candidatos
    archivos = sorted(
        [
            f
            for f in os.listdir(folder_in)
            if f.lower().startswith(("ts_", "tmax_", "tmin_"))
            and f.lower().endswith(".csv")
        ]
    )
    if not archivos:
        print(
            "No se encontraron archivos ts_*, tmax_*, tmin_* en la carpeta de entrada."
        )
        return

    print(f"\nSe procesarán {len(archivos)} archivos encontrados.\n")

    logs_global = []

    try:
        for i, archivo in enumerate(archivos, start=1):
            print(
                f"\n========== [{i}/{len(archivos)}] Procesando: {archivo} =========="
            )
            ruta_in = os.path.join(folder_in, archivo)

            # verificar si ya fue procesado (existe _QC.csv)
            archivo_qc = archivo.replace(".csv", "_QC.csv")
            ruta_salida_existente = os.path.join(folder_out, archivo_qc)
            cambios = load_changes(folder_out)
            ya_existe = os.path.exists(ruta_salida_existente)

            completed = load_completed(folder_out)
            ya_completado = archivo in completed["completadas"]

            if ya_completado:
                print(f"⏩ Archivo {archivo} ya revisado completamente. Se omite.")
                continue

            aplicar_cambios_json_previos = False
            if ya_existe:
                print(f"\n⚠️ Se encontró una versión QC previa para: {archivo}")
                while True:
                    resp = (
                        input(
                            "¿Qué desea hacer?\n"
                            "  (s) Omitir y marcar como COMPLETADO\n"
                            "  (n) Revisar nuevamente desde cero\n"
                            "  (p) Posponer — omitir solo en esta ejecución\n"
                            "Elija opción: "
                        )
                        .strip()
                        .lower()
                    )

                    if resp in ("s", "n", "p"):
                        break
                    print("❌ Opción inválida. Intente de nuevo.")

                if resp == "s":
                    # marcar como completado
                    completed = load_completed(folder_out)
                    if archivo not in completed["completadas"]:
                        completed["completadas"].append(archivo)
                        save_completed(folder_out, completed)
                    print(f"✔ Marcado como completado: {archivo}")
                    continue

                elif resp == "p":
                    print(f"⏭ Omitido únicamente en esta ejecución: {archivo}")
                    continue

                elif resp == "n":
                    print(f"🔄 Se revisará de nuevo desde cero: {archivo}")
                    # NO ponemos continue → avanza al procesamiento normal

                    # Buscar si existen cambios previos en JSON para este archivo
                    cambios_previos = [
                        ent
                        for ent in cambios.get("single_changes", [])
                        if ent.get("archivo") == archivo
                    ] + [
                        ent
                        for ent in cambios.get("swaps", [])
                        if ent.get("archivo_1") == archivo
                        or ent.get("archivo_2") == archivo
                    ]

                    if cambios_previos:
                        print(
                            f"⚠️ Se encontraron {len(cambios_previos)} cambios previos en el JSON para {archivo}."
                        )
                        aplicar_previos = (
                            input(
                                "¿Desea aplicar estos cambios antes de iniciar el QC? (s/n): "
                            )
                            .strip()
                            .lower()
                        )

                        if aplicar_previos in ("s", "y"):
                            print(
                                "🔧 Se aplicarán los cambios previos al archivo original."
                            )
                            aplicar_cambios_json_previos = True
                        else:
                            print("⏭️ Se omitirá la aplicación de cambios previos.")
                            aplicar_cambios_json_previos = False
                    else:
                        aplicar_cambios_json_previos = False

            # verificar si fue modificado por un swap previo
            fue_modificado_indirectamente = any(
                archivo in (ent.get("archivo_1"), ent.get("archivo_2"))
                for ent in cambios.get("swaps", [])
            )
            if fue_modificado_indirectamente:
                print(
                    f"🔁 {archivo} fue modificado por un swap previo, pero se revisará igualmente."
                )

            # procesar archivo (con control térmico + estadístico)
            logs_local = procesar_archivo_interactivo(
                ruta_in,
                folder_out,
                lp,
                up,
                k,
                ventana=7,
                aplicar_previos=aplicar_cambios_json_previos,
            )

            # consolidar log local al global
            if logs_local:
                for entry in logs_local:
                    # unificar columnas para trazabilidad térmica
                    entry.setdefault("tipo_inconsistencia", "")
                    entry.setdefault("accion_termica", "")
                    entry.setdefault("valores_previos", {})
                    entry.setdefault("valores_nuevos", {})
                    entry.setdefault("accion", "")
                    entry.setdefault(
                        "fecha_proceso", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                    logs_global.append(entry)

    except KeyboardInterrupt:
        print("\n🟡 Interrupción manual detectada. Guardando log parcial...")
        if logs_global:
            df_log = pd.DataFrame(logs_global)
            ruta_log_parcial = os.path.join(folder_out, "log_anomalias_parcial.csv")
            df_log.to_csv(ruta_log_parcial, index=False)
            print(f"📁 Log parcial guardado en: {ruta_log_parcial}")
        print("Proceso interrumpido de forma segura.")
        return

    # --- guardar log global completo ---
    if logs_global:
        df_log = pd.DataFrame(logs_global)
        ruta_log = os.path.join(folder_out, "log_anomalias.csv")

        # ordenar columnas principales
        columnas_preferidas = [
            "archivo",
            "fecha",
            "tipo_inconsistencia",
            "accion_termica",
            "valor_original",
            "valor_nuevo",
            "accion",
            "valores_previos",
            "valores_nuevos",
            "percentil_inferior",
            "percentil_superior",
            "k_iqr",
            "fecha_proceso",
        ]
        cols_final = [c for c in columnas_preferidas if c in df_log.columns] + [
            c for c in df_log.columns if c not in columnas_preferidas
        ]
        df_log = df_log[cols_final]

        df_log.to_csv(ruta_log, index=False)
        print(f"\n📜 Log global de control de calidad guardado en: {ruta_log}")
    else:
        print("\nNo se registraron acciones (log vacío).")

    print("\n✅ Procesamiento por lote completado exitosamente.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python qc_batch_temperature.py <carpeta_entrada> <carpeta_salida>")
        sys.exit(1)
    folder_in = sys.argv[1]
    folder_out = sys.argv[2]
    main_batch(folder_in, folder_out)
