#!/usr/bin/env python3
"""
workflow.py

Flujo completo del control de calidad de temperaturas.
Incluye:

 - Detección de archivo base (org/tmp/qc)
 - Control termodinámico interactivo
 - Control estadístico
 - Gráficas en contexto (tmax, tmean, tmin, pr)
 - Comparación manual con otras estaciones
 - Escritura _tmp y _qc
 - Archivo auxiliar *_changes.csv
 - Generación de reportes PDF
"""

from pathlib import Path
import pandas as pd
import json

from qc_batch.io_manager import (
    find_candidate_file,
    read_series,
    write_qc,
    build_filename,
)
from qc_batch.thermo_qc import (
    load_triplet,
    detect_thermal_inconsistencies,
    apply_thermal_correction,
    write_triplet_tmp,
)
from qc_batch.stat_qc import compute_bounds, detect_outliers, apply_statistical_decision
from qc_batch.visualization import plot_context_2x2, plot_comparison_qc
from qc_batch.helpers_compare import compare_with_other_station
from qc_batch.modifications import build_changes_dataframe, save_changes_csv
from qc_batch.report import generar_informe_pdf
import matplotlib.pyplot as plt
import os


# ================================================================
#  Registrar archivo completado
# ================================================================


def mark_completed(folder_out: str, filename: str):
    path = Path(folder_out) / "completed_series.json"

    if not path.exists():
        data = {"completadas": []}
    else:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except:
            data = {"completadas": []}

    if filename not in data["completadas"]:
        data["completadas"].append(filename)

    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def sugerir_accion_letras(tmin, tmean, tmax):
    # Caso más frecuente que mencionaste (duplicaciones)
    if tmean == tmin or tmean == tmax:
        return "t", "Poner tmean = -99 (valor duplicado o inconsistente)."

    # Inversión térmica
    if tmax < tmin:
        return "i", "Intercambiar tmin ↔ tmax (inversión detectada)."

    # Tmean fuera de rango
    if tmean > tmax or tmean < tmin:
        return "t", "Poner tmean = -99 (valor fuera de rango)."

    # Ninguna sugerencia automática
    return None, None


def sugerir_accion_outlier(valor, p_low, p_high, iqr):
    # Outlier extremo (más de 3 IQR)
    if valor < p_low - 3 * iqr or valor > p_high + 3 * iqr:
        return "s", "El valor es un outlier extremo. Sugerencia: reemplazar por -99."

    # Outlier moderado (entre 1.5 y 3 IQR)
    if valor < p_low - 1.5 * iqr or valor > p_high + 1.5 * iqr:
        return "n", "Fuera de rango moderado. Sugerencia: ingresar un valor corregido."

    # Outlier leve
    return (
        "m",
        "El valor está ligeramente fuera del rango. Sugerencia: mantener (inspección manual recomendada).",
    )


# ============================================================
# CARGA INTELIGENTE DE SERIES PARA REVISIÓN PARCIAL (start_from="qc")
# Prioridad: 1) QC  2) TMP  3) ORG
# Incluye PR (que antes NO se cargaba).
# ============================================================


def _load_triplet_from_qc(folder_out, folder_in, periodo, estacion):
    estacion = estacion.upper()
    vars_all = ["tmin", "tmean", "tmax", "pr"]
    result = {}
    paths_trip = {}

    for var in vars_all:
        path_qc = Path(folder_out) / build_filename(var, periodo, estacion, "qc")
        path_tmp = Path(folder_out) / build_filename(var, periodo, estacion, "tmp")
        path_org = Path(folder_in) / build_filename(var, periodo, estacion, "org")

        df = None
        origen = "N/D"

        if path_qc.exists():
            try:
                df = read_series(str(path_qc))
                origen = str(path_qc)
            except:
                df = None

        if df is None and path_tmp.exists():
            try:
                df = read_series(str(path_tmp))
                origen = str(path_tmp)
            except:
                df = None

        if df is None and path_org.exists():
            try:
                df = read_series(str(path_org))
                origen = str(path_org)
            except:
                df = None

        result[var] = df
        paths_trip[var] = origen

    return result, paths_trip


def load_triplet_from_org(folder_in, periodo, estacion):
    """
    Carga tmin, tmean, tmax y pr desde la carpeta ORG con fallbacks:
      - intenta nombres exactos con sufijo _org
      - si var == 'tmean' intenta también 'ts'
      - intenta archivos sin sufijo (compatibilidad)
      - intenta patrón flexible var_*_{estacion}*.csv
    Retorna (dfs, paths)
    """
    from pathlib import Path
    from qc_batch.io_manager import build_filename, normalize_var_name, read_series

    folder_in = Path(folder_in)
    estacion_up = estacion.upper()

    vars_trip = ["tmin", "tmean", "tmax", "pr"]
    dfs = {}
    paths = {}

    for v in vars_trip:
        tried = []
        df = None
        path_found = "N/D"

        # 1) Nombre exacto con sufijo _org
        fname = build_filename(v, periodo, estacion_up, "org")
        p = folder_in / fname
        tried.append(str(p))
        if p.exists():
            try:
                df = read_series(str(p))
                path_found = str(p)
            except Exception:
                df = None

        # 2) Si var == tmean intentar 'ts'
        if df is None and v == "tmean":
            alt_fname = build_filename("ts", periodo, estacion_up, "org")
            p2 = folder_in / alt_fname
            tried.append(str(p2))
            if p2.exists():
                try:
                    df = read_series(str(p2))
                    path_found = str(p2)
                except Exception:
                    df = None

        # 3) Intentar archivo sin sufijo: var_periodo_estacion.csv
        if df is None:
            alt3 = folder_in / f"{v}_{periodo}_{estacion_up}.csv"
            tried.append(str(alt3))
            if alt3.exists():
                try:
                    df = read_series(str(alt3))
                    path_found = str(alt3)
                except Exception:
                    df = None

        # 4) Intentar patrón flexible (cualquier periodo) var_*_{estacion}*.csv (org o no)
        if df is None:
            pattern = f"{v}_*_{estacion_up}*.csv"
            matches = sorted(folder_in.glob(pattern))
            if matches:
                for m in matches:
                    tried.append(str(m))
                    try:
                        df = read_series(str(m))
                        path_found = str(m)
                        break
                    except Exception:
                        df = None

        # 5) para tmean intentar patrón con 'ts' también
        if df is None and v == "tmean":
            pattern_ts = f"ts_*_{estacion_up}*.csv"
            matches_ts = sorted(folder_in.glob(pattern_ts))
            if matches_ts:
                for m in matches_ts:
                    tried.append(str(m))
                    try:
                        df = read_series(str(m))
                        path_found = str(m)
                        break
                    except Exception:
                        df = None

        # asignar resultados
        dfs[v] = df
        paths[v] = path_found

    return dfs, paths


# ================================================================
#  Función principal
# ================================================================


def process_file(
    var: str,
    periodo: str,
    estacion: str,
    folder_in: str,
    folder_out: str,
    lower_p: float = 0.1,
    upper_p: float = 0.9,
    k: float = 1.5,
    ventana: int = 7,
    ask_user=None,
    start_from: str = "auto",
):
    """
    Procesa una variable para una estación específica.
    """

    # Normalizar ID de estación (case-sensitive filesystem)
    estacion = estacion.upper()

    if ask_user is None:
        ask_user = input

    paths_loaded = {}

    # Excluir variables NO térmicas (solo pr)
    if var.lower() not in ("tmax", "tmean", "tmin"):
        print(f"⏭ Omitiendo variable no térmica: {var}")
        return

    # Archivo base
    base_info = None
    status = None
    path_base = None

    if start_from in ("auto", "qc"):
        base_info = find_candidate_file(folder_in, folder_out, var, periodo, estacion)
        status = base_info.get("status")
        path_base = base_info.get("path")

    # ⚠️ SOLO validar existencia en modo AUTO o QC
    if start_from in ("auto", "qc") and status is None:
        print(f"No se encontró archivo para {var}_{periodo}_{estacion}")
        return

    # --- bandera para evitar recarga accidental del triplete ---
    triplet_loaded = False

    # Forzar archivo ORG si se indicó explícitamente desde main_batch
    if start_from == "org":
        status = "org"
        path_base = Path(folder_in) / build_filename(var, periodo, estacion, "org")
        print(f"[{var.upper()}] Forzando carga desde ORG: {path_base}")
        # Cargar TMIN, TMEAN, TMAX y PR exclusivamente desde ORG
        dfs_trip, paths_trip = load_triplet_from_org(folder_in, periodo, estacion)
        triplet_loaded = True

        # Cargar la variable principal desde ORG y sobrescribir solo esa variable
        df_var_base = read_series(str(path_base))
        dfs_trip[var] = df_var_base

    # Forzar QC si se pidió explícitamente revisión parcial
    elif start_from == "qc":
        status = "qc"
        path_base = Path(folder_out) / build_filename(var, periodo, estacion, "qc")
        print(f"[{var.upper()}] Revisión parcial usando QC: {path_base}")

    else:
        if status and path_base:
            print(
                f"[{var.upper()}] Archivo base detectado: {status.upper()} → {path_base}"
            )

    # Registrar ruta real del archivo cargado para la variable principal
    paths_loaded[var] = str(path_base)

    # Leer ORG para comparativa final (buscar org o fallback sin sufijo; aceptar alias ts<->tmean)
    path_org = Path(folder_in) / build_filename(var, periodo, estacion, "org")
    if not path_org.exists():
        # intentar archivo sin sufijo
        alt1 = Path(folder_in) / f"{var}_{periodo}_{estacion}.csv"
        if alt1.exists():
            path_org = alt1
        else:
            # si var es tmean, intentar ts y viceversa
            if var.lower() == "tmean":
                alt2 = Path(folder_in) / build_filename("ts", periodo, estacion, "org")
                alt3 = Path(folder_in) / f"ts_{periodo}_{estacion}.csv"
                if alt2.exists():
                    path_org = alt2
                elif alt3.exists():
                    path_org = alt3
            elif var.lower() == "ts":
                alt2 = Path(folder_in) / build_filename(
                    "tmean", periodo, estacion, "org"
                )
                alt3 = Path(folder_in) / f"tmean_{periodo}_{estacion}.csv"
                if alt2.exists():
                    path_org = alt2
                elif alt3.exists():
                    path_org = alt3

    if not path_org.exists():
        # si no hay org disponible, intentar usar candidate org in folder_in
        # buscar cualquier archivo en folder_in que coincida en var/periodo/estacion
        candidates = list(Path(folder_in).glob(f"{var}_*_{estacion}*.csv"))
        if candidates:
            path_org = candidates[0]

    try:
        df_org = read_series(str(path_org))
    except Exception:
        # si no se pudo leer, crear df_org vacío con fechas del df_base si existe
        df_org = None

    # ============================================================
    # MODO FORZADO ORG: impedir cualquier detección automática
    # ============================================================
    if start_from == "org":
        # El triplete YA fue cargado exclusivamente desde ORG.
        # No se permite fallback a TMP ni QC en ningún punto.
        pass

    # ------------------------------------------
    # Cargar triplete SOLO si no se cargó antes
    # ------------------------------------------
    if not triplet_loaded:
        if start_from == "qc":
            dfs_trip, paths_trip = _load_triplet_from_qc(
                folder_out, folder_in, periodo, estacion
            )
        elif start_from == "org":
            # Ya fue cargado antes → NO recargar
            pass
        else:
            dfs_trip, paths_trip = load_triplet(
                folder_in, folder_out, periodo, estacion, force_org=False
            )

    # Registrar rutas reales para TMIN, TMEAN, TMAX y PR (usar paths_trip ya poblado)
    for v_local in ["tmin", "tmean", "tmax", "pr"]:
        paths_loaded[v_local] = paths_trip.get(v_local, "N/D")

    # ============================================================
    # 📝 Mostrar origen de cada dataset cargado en el triplete
    # ============================================================

    print("\n📂 Archivos cargados para esta sesión:")

    for v_local in ["tmin", "tmean", "tmax", "pr"]:
        df = dfs_trip.get(v_local)
        ruta = paths_loaded.get(v_local, "N/D")

        if df is None or df.empty:
            print(f"   • {v_local.upper():<5} → SIN DATOS → {ruta}")
        else:
            fmin = df["fecha"].min().date()
            fmax = df["fecha"].max().date()
            print(
                f"   • {v_local.upper():<5} → filas={len(df)}, "
                f"rango=({fmin} → {fmax}) → {ruta}"
            )

    # =========================================================
    #  CONTROL TERMODINÁMICO (BUCLE DINÁMICO CORREGIDO)
    # =========================================================

    # Detectar inconsistencias iniciales
    inconsist = detect_thermal_inconsistencies(
        dfs_trip["tmin"], dfs_trip["tmean"], dfs_trip["tmax"]
    )

    # Inicializar resumen térmico
    resumen_termico = []

    total_inconsist = len(inconsist)
    print(f"🌡 Se detectaron {total_inconsist} inconsistencias térmicas iniciales.")
    corregidas = 0

    # ===============================================
    # ❓ Preguntar si continuar o saltar este archivo
    # ===============================================
    if total_inconsist > 0:
        resp = (
            ask_user(
                f"\n⚠️  Este archivo tiene {total_inconsist} inconsistencias térmicas.\n"
                "¿Desea revisarlas ahora? (s = sí, n = dejar para después): "
            )
            .strip()
            .lower()
        )

        # Si responde "n", "no", o cualquier cosa que no sea sí → skip inmediato
        if resp not in ("s", "y", ""):
            print("\n⏭ Archivo omitido por decisión del usuario.\n")
            return dfs_trip  # ← No hace revisión térmica

    # 🔁 Bucle dinámico: mientras existan inconsistencias, procesarlas
    while len(inconsist) > 0:

        # Tomar SOLO la primera inconsistencia pendiente
        inc = inconsist[0]
        fecha = pd.to_datetime(inc["fecha"])
        tipo = inc["tipo"]

        print(f"\nInconsistencia térmica en {fecha.date()} → {tipo}")

        # Mostrar gráfica de contexto
        fig = plot_context_2x2(
            dfs_trip,
            var_principal=var,
            estacion=estacion,
            fecha_obj=fecha,
            ventana=ventana,
            tipo_inconsistencia=tipo,
            folder_out=folder_out,
            show=True,
        )

        # Comparar solo si el usuario realmente lo desea, UNA o VARIAS veces
        while ask_user(
            "¿Desea comparar con otra estación para el mismo día? (s/n): "
        ).strip().lower() in ("s", "y"):

            estacion_comp = ask_user(
                "Ingrese el ID de la estación (ej. S-12): "
            ).strip()

            if estacion_comp:
                compare_with_other_station(
                    var,
                    periodo,
                    estacion,
                    fecha,
                    folder_in,
                    folder_out,
                    ventana,
                    ask_user,
                    prompt_first=False,
                    estacion_comp=estacion_comp,
                )
            else:
                print("⚠ Código de estación vacío. Omitiendo.\n")

        # -----------------------------------------------------------
        # Merge robusto del triplete (TMEAN es opcional)
        # -----------------------------------------------------------

        # Extraer cada variable térmica de manera segura
        df_tmin = dfs_trip.get("tmin")
        df_tmean = dfs_trip.get("tmean")
        df_tmax = dfs_trip.get("tmax")

        # Asegurar DataFrames consistentes
        if df_tmin is not None and not df_tmin.empty:
            df_tmin = df_tmin[["fecha", "valor"]].rename(columns={"valor": "tmin"})
        else:
            df_tmin = None

        if df_tmean is not None and not df_tmean.empty:
            df_tmean = df_tmean[["fecha", "valor"]].rename(columns={"valor": "tmean"})
        else:
            df_tmean = None  # TMEAN es opcional

        if df_tmax is not None and not df_tmax.empty:
            df_tmax = df_tmax[["fecha", "valor"]].rename(columns={"valor": "tmax"})
        else:
            df_tmax = None

        # Iniciar merge con la primera variable disponible
        if df_tmin is not None:
            trip = df_tmin.copy()
        elif df_tmean is not None:
            trip = df_tmean.copy()
        elif df_tmax is not None:
            trip = df_tmax.copy()
        else:
            print("⚠ No hay datos térmicos disponibles para generar el triplete.")
            trip = pd.DataFrame(columns=["fecha", "tmin", "tmean", "tmax"])

        # Agregar TMEAN si existe
        if df_tmean is not None:
            trip = trip.merge(df_tmean, on="fecha", how="outer")

        # Agregar TMAX si existe
        if df_tmax is not None:
            trip = trip.merge(df_tmax, on="fecha", how="outer")

        # Ordenar por fecha y resetear índice
        trip = trip.sort_values("fecha").reset_index(drop=True)

        # Extraer fila exacta para la inconsistencia
        vals = trip.loc[trip["fecha"] == fecha]

        # Validación: si falta valor se toma como -99 explícito (no mezcla datos viejos)
        # Usar .get para proteger contra columnas faltantes tras merge
        def _safe_get(vals_df, col):
            if col in vals_df.columns:
                return float(vals_df[col].fillna(-99).iloc[0])
            return -99.0

        tmin_val = _safe_get(vals, "tmin")
        tmean_val = _safe_get(vals, "tmean")
        tmax_val = _safe_get(vals, "tmax")

        # Reglas de validez
        validos = [v != -99 for v in (tmin_val, tmean_val, tmax_val)]
        num_validos = sum(validos)

        if num_validos >= 2:
            hay_inconsistencia_real = (
                (
                    tmin_val != -99 and tmax_val != -99 and tmax_val < tmin_val
                )  # inversión
                or (
                    tmean_val != -99 and tmax_val != -99 and tmean_val > tmax_val
                )  # tmean > tmax
                or (
                    tmean_val != -99 and tmin_val != -99 and tmean_val < tmin_val
                )  # tmean < tmin
                or (
                    tmean_val != -99 and tmin_val != -99 and tmean_val == tmin_val
                )  # ❗ igualdad inválida 1
                or (
                    tmean_val != -99 and tmax_val != -99 and tmean_val == tmax_val
                )  # ❗ igualdad inválida 2
                or (
                    tmin_val != -99 and tmax_val != -99 and tmin_val == tmax_val
                )  # ❗ igualdad inválida 3
            )

        else:
            hay_inconsistencia_real = False

        if not hay_inconsistencia_real:
            # Corregida “solo por recalcular”
            inconsist = detect_thermal_inconsistencies(
                dfs_trip["tmin"], dfs_trip["tmean"], dfs_trip["tmax"]
            )
            continue

        # ============================
        # Menú térmico (igual que antes)
        # ============================
        fecha_str = fecha.strftime("%Y-%m-%d")
        print("\n-----------------------------------------")
        print(f"❗ Inconsistencia térmica detectada en {fecha_str}")
        print("-----------------------------------------")
        print(f"   🌡 tmin  = {tmin_val}")
        print(f"   🌡 tmean = {tmean_val}")
        print(f"   🌡 tmax  = {tmax_val}\n")

        sug, msg = sugerir_accion_letras(tmin_val, tmean_val, tmax_val)
        if sug:
            print(f"💡 Sugerencia automática: {msg}")
            print(f"   → Presione ENTER para aceptar ({sug}).\n")

        print("Acciones disponibles:")
        print("   (i) 🔄 Intercambiar tmin ↔ tmax")
        print("   (t) ❌ Establecer solo tmean = -99   [RECOMENDADO]")
        print("   (u) ❌ Establecer solo tmax = -99")
        print("   (l) ❌ Establecer solo tmin = -99")
        print("   (x) 🚫 Establecer tmean y otra variable en -99")
        print("   (e) ✏  Editar manualmente tmin / tmean / tmax")
        print("   (r) 🧹 Reordenar automáticamente (tmin < tmean < tmax)")
        print("   (m) 👍 Mantener valores")
        print("   (s) 🗑  Establecer los 3 valores en -99")
        print("   (p) ⏭  Pasar sin hacer cambios\n")

        op = ask_user("Seleccione una acción: ").strip().lower()
        if op == "" and sug:
            op = sug

        while op not in ("i", "t", "x", "e", "s", "m", "r", "p", "u", "l"):
            print("❌ Acción inválida.")
            op = ask_user("Seleccione una acción: ").strip().lower()
            if op == "" and sug:
                op = sug

        # Aplicar corrección térmica
        dfs_trip = apply_thermal_correction(
            op, fecha, dfs_trip, folder_out, estacion, periodo
        )

        resumen_termico.append({"fecha": fecha_str, "tipo": tipo, "accion": op})

        # ✔ Cerrar todas las ventanas abiertas
        try:
            plt.close("all")
        except:
            pass

        # Guardar TMP coherente después de *cada* corrección
        write_triplet_tmp(dfs_trip, paths_trip, folder_out, estacion)
        print(f"💾 TMP actualizado para {fecha_str}.")

        # 🔁 Recalcular inconsistencias con el triplete ACTUALIZADO
        inconsist_new = detect_thermal_inconsistencies(
            dfs_trip["tmin"], dfs_trip["tmean"], dfs_trip["tmax"]
        )

        # Si la inconsistencia sigue EXACTAMENTE igual después de aplicar la acción → evitar loop infinito
        if any(
            inc2["fecha"] == fecha and inc2["tipo"] == tipo for inc2 in inconsist_new
        ):
            print(
                f"⚠ Advertencia: la inconsistencia en {fecha_str} no cambió después de la acción."
            )
            print("   No se repetirá este ciclo para evitar un loop infinito.\n")
            # Forzar salida de esta inconsistencia:
            inconsist = [inc for inc in inconsist_new if inc["fecha"] != fecha]
            continue

        if not any(inc["fecha"] == fecha for inc in inconsist_new):
            corregidas += 1
            print(
                f"✔ Inconsistencia corregida para {fecha_str}. "
                f"Progreso: {corregidas}/{total_inconsist}\n"
            )

        # Actualizar lista y repetir WHILE si quedan inconsistencias
        inconsist = inconsist_new

    # Guardar *_tmp.csv
    # Si entramos en modo QC parcial (start_from == "qc") NO escribir tmp (dejamos QC como origen);
    # si entramos desde 'auto' o normal, sí escribimos tmp.
    if start_from != "qc":
        write_triplet_tmp(dfs_trip, paths_trip, folder_out, estacion)
    else:
        # aún así, si hubo modificaciones guardadas, escribir QC final después del bloque estadístico
        pass
    print("\n===== RESUMEN DE CORRECCIONES TÉRMICAS =====")
    for item in resumen_termico:
        print(f" • {item['fecha']}  →  {item['tipo']}  → acción '{item['accion']}'")
    print("===========================================\n")

    # DF base de la variable
    # Recuperar siempre la serie original corregida SIN merges
    df_base = dfs_trip[var][["fecha", "valor"]].copy()

    # Si quedó vacía, intentar recuperar desde el archivo base original
    if df_base.empty:
        print("⚠ df_base vacío tras control térmico — recuperando desde ORG...")
        df_base = read_series(str(path_org))

    # =========================================================
    #  CONTROL ESTADÍSTICO
    # =========================================================

    # PR no participa en el análisis estadístico
    if var.lower() == "pr":
        print(
            "⏭ PR se usa solo como referencia en gráficas — se omite control estadístico."
        )
        outliers = []
        total_outliers = 0
        resumen_outliers = []
        bounds = {"p_low": None, "p_high": None, "iqr": None}
    else:
        # Validar serie
        if df_base is None or df_base.empty:
            print(
                "⚠ No hay datos para evaluar estadísticamente. Se omite control estadístico."
            )
            outliers = []
            total_outliers = 0
            resumen_outliers = []
            bounds = {"p_low": None, "p_high": None, "iqr": None}
        else:
            # Serie válida (sin -99)
            serie_valida = df_base[df_base["valor"] != -99]["valor"]

            # Calcular límites
            bounds = compute_bounds(serie_valida, lower_p=lower_p, upper_p=upper_p, k=k)

            # Detectar outliers
            outliers = detect_outliers(df_base, bounds)
            total_outliers = len(outliers)
            resumen_outliers = []

            print(f"📊 Se detectaron {total_outliers} outliers estadísticos.")

    # =========================================================
    #  PROCESAMIENTO INTERACTIVO (MENÚ ESTADÍSTICO)
    # =========================================================

    corregidos_est = 0

    # PR no debe entrar al menú estadístico
    if var.lower() == "pr":
        return

    for idx, val in outliers:

        fecha = df_base.loc[idx, "fecha"]
        fecha_str = fecha.strftime("%Y-%m-%d")
        valor = val

        print(f"\nOutlier estadístico en {fecha_str} → {valor}")

        # Mostrar contexto completo
        fig = plot_context_2x2(
            dfs_trip,
            var_principal=var,
            estacion=estacion,
            fecha_obj=fecha,
            ventana=ventana,
            tipo_inconsistencia="estadistico",
            folder_out=folder_out,
            show=True,
        )

        # Comparación por ID con otras estaciones
        compare_with_other_station(
            var, periodo, estacion, fecha, folder_in, folder_out, ventana, ask_user
        )

        # No puede usarse bounds si era PR o si no había datos
        if bounds["p_low"] is not None:
            p_low = bounds["p_low"]
            p_high = bounds["p_high"]
            iqr = bounds["iqr"]

            # Sugerencia automática basada en reglas estadísticas
            sug, msg = sugerir_accion_outlier(valor, p_low, p_high, iqr)

            print("\n-----------------------------------------")
            print(f"📊 Outlier estadístico detectado en {fecha_str}")
            print("-----------------------------------------")
            print(f"   Valor observado : {valor}")
            print(f"   Rango esperado  : {p_low:.2f} – {p_high:.2f}")
            print(f"   IQR             : {iqr:.2f}\n")

            if sug:
                print(f"💡 Sugerencia automática: {msg}")
                print(f"   → Presione ENTER para aceptar ({sug}).\n")
        else:
            sug = None
            print("\n(No se generaron límites estadísticos para esta variable.)\n")

        # Menú interactivo
        print("Acciones disponibles:")
        print("   (s) ❌ Sustituir SOLO este valor por -99")
        print("   (m) 👍 Mantener valor original")
        print("   (n) ✏  Ingresar nuevo valor manualmente")
        print("   (p) ⏭  Pasar sin hacer cambios")
        print("\n   --- Opciones adicionales ---")
        print("   (1) ❌ Sustituir TMIN por -99")
        print("   (2) ❌ Sustituir TMAX por -99")
        print("   (3) ❌ Sustituir TMEAN por -99")
        print("   (a) ❌ Sustituir TMIN, TMAX y TMEAN por -99")
        print()

        # Capturar acción del usuario
        action = ask_user("Seleccione acción: ").strip().lower()

        # ENTER acepta sugerencia automática (si existe)
        if action == "" and sug:
            action = sug

        # Validar entrada
        valid_actions = ("s", "m", "n", "p", "1", "2", "3", "a")
        while action not in valid_actions:
            print("❌ Acción inválida.")
            action = ask_user("Seleccione acción: ").strip().lower()
            if action == "" and sug:
                action = sug

        # Aplicar decisión
        if action == "n":
            # ingresar nuevo valor
            nuevo = float(ask_user(f"Nuevo valor para {fecha_str}: ").strip())
            df_base = apply_statistical_decision(df_base, idx, "n", nuevo)
            resumen_outliers.append(
                {"fecha": fecha_str, "valor": valor, "accion": "n", "nuevo": nuevo}
            )

        elif action == "1":
            # Sustituir TMIN por -99 en la fecha correspondiente
            fecha_mask = dfs_trip["tmin"]["fecha"] == fecha
            if fecha_mask.any():
                dfs_trip["tmin"].loc[fecha_mask, "valor"] = -99
            else:
                # crear fila si no existe
                dfs_trip["tmin"].loc[len(dfs_trip["tmin"])] = {
                    "fecha": fecha,
                    "valor": -99,
                }
            resumen_outliers.append({"fecha": fecha_str, "accion": "tmin=-99"})

        elif action == "2":
            fecha_mask = dfs_trip["tmax"]["fecha"] == fecha
            if fecha_mask.any():
                dfs_trip["tmax"].loc[fecha_mask, "valor"] = -99
            else:
                dfs_trip["tmax"].loc[len(dfs_trip["tmax"])] = {
                    "fecha": fecha,
                    "valor": -99,
                }
            resumen_outliers.append({"fecha": fecha_str, "accion": "tmax=-99"})

        elif action == "3":
            # TMEAN
            if dfs_trip.get("tmean") is None:
                dfs_trip["tmean"] = pd.DataFrame(columns=["fecha", "valor"])
            fecha_mask = dfs_trip["tmean"]["fecha"] == fecha
            if fecha_mask.any():
                dfs_trip["tmean"].loc[fecha_mask, "valor"] = -99
            else:
                dfs_trip["tmean"].loc[len(dfs_trip["tmean"])] = {
                    "fecha": fecha,
                    "valor": -99,
                }
            resumen_outliers.append({"fecha": fecha_str, "accion": "tmean=-99"})

        elif action == "a":
            # Sustituir los tres por -99
            for vv in ("tmin", "tmean", "tmax"):
                if dfs_trip.get(vv) is None:
                    dfs_trip[vv] = pd.DataFrame(columns=["fecha", "valor"])
                fecha_mask = dfs_trip[vv]["fecha"] == fecha
                if fecha_mask.any():
                    dfs_trip[vv].loc[fecha_mask, "valor"] = -99
                else:
                    dfs_trip[vv].loc[len(dfs_trip[vv])] = {"fecha": fecha, "valor": -99}
            resumen_outliers.append(
                {"fecha": fecha_str, "accion": "tmin,tmax,tmean=-99"}
            )

        else:
            # acciones s, m, p se manejan igual que siempre
            df_base = apply_statistical_decision(df_base, idx, action)
            resumen_outliers.append(
                {"fecha": fecha_str, "valor": valor, "accion": action}
            )

        # Asegurar df_base refleje correcciones si la variable principal fue alterada
        df_base = dfs_trip[var][["fecha", "valor"]].copy()

        corregidos_est += 1
        print(f"✔ Outlier corregido. Progreso: {corregidos_est}/{total_outliers}\n")

        # Actualizar serie principal
        dfs_trip[var] = df_base

        for vv in ("tmin", "tmean", "tmax"):
            if dfs_trip.get(vv) is not None:
                dfs_trip[vv]["fecha"] = pd.to_datetime(dfs_trip[vv]["fecha"])
                dfs_trip[vv] = dfs_trip[vv].sort_values("fecha").reset_index(drop=True)

        # Registrar cambio en log (no crítico)
        try:
            from qc_batch.thermo_qc import _load_changes, _save_changes

            changes = _load_changes(folder_out)
            entry = {
                "timestamp": pd.Timestamp.now(tz=None).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "estacion": estacion,
                "fecha": fecha_str,
                "accion": action,
                "valor_prev": float(valor),
                "valor_new": (
                    None
                    if action == "s"
                    else (float(nuevo) if action == "n" else float(valor))
                ),
                "nota": "decisión estadística (workflow)",
            }
            changes.setdefault("single_changes", []).append(entry)
            _save_changes(folder_out, changes)
        except Exception:
            pass

        # Cerrar ventanas gráficas
        try:
            plt.close("all")
        except:
            pass

    print("\n===== RESUMEN DE OUTLIERS ESTADÍSTICOS =====")
    for item in resumen_outliers:
        print(f" • {item['fecha']} → acción '{item['accion']}'")
    print("============================================\n")

    # =========================================================
    # GUARDAR ARCHIVO QC
    # =========================================================
    if var.lower() != "pr":
        path_qc = write_qc(df_base, folder_out, var, periodo, estacion)

    print(f"\n✔ Archivo QC generado: {path_qc}")

    mark_completed(folder_out, Path(path_qc).name)

    # ===========================================
    # 🧹 Eliminar SOLO el TMP de la variable revisada
    # ===========================================
    tmp_file = Path(folder_out) / build_filename(var, periodo, estacion, "tmp")

    if tmp_file.exists():
        try:
            tmp_file.unlink()
            print(f"🧹 TMP eliminado: {tmp_file.name}")
        except Exception as e:
            print(f"⚠ No se pudo eliminar {tmp_file}: {e}")

    # =========================================================
    # ARCHIVO DE CAMBIOS
    # =========================================================
    if df_org is None:
        print("⚠ No fue posible cargar ORG; se omite tabla de cambios.")
        df_changes = pd.DataFrame()
    else:
        df_changes = build_changes_dataframe(df_org, df_base, folder_out)

    path_changes = save_changes_csv(df_changes, folder_out, var, periodo, estacion)
    print(f"📄 Archivo de cambios generado: {path_changes}")

    # =========================================================
    # GRÁFICA COMPARATIVA
    # =========================================================
    plot_comparison_qc(df_org, df_base, var, periodo, estacion, folder_out)

    # =========================================================
    # INFORME PDF
    # =========================================================
    generar_informe_pdf(folder_out, var, periodo, estacion, df_changes)

    print(f"\n🎉 [OK] QC COMPLETADO para {var.upper()} en estación {estacion}.\n")


def auditar_qc(
    var, periodo, estacion, folder_in, folder_out, lower_p=0.1, upper_p=0.9, k=1.5
):
    """Audita un archivo QC existente (térmico + estadístico) sin modificarlo.
    Devuelve un informe dict y permite optar por entrar a corrección parcial llamando a process_file.
    """
    # cargar triplete desde QC explícitamente si existe
    from qc_batch.io_manager import build_filename

    dfs = {}
    for v in ("tmin", "tmean", "tmax"):
        p = Path(folder_out) / build_filename(v, periodo, estacion, "qc")
        if p.exists():
            try:
                dfs[v] = read_series(str(p))
            except Exception:
                dfs[v] = None
        else:
            dfs[v] = None

    # Auditoría térmica
    inconsistencias = detect_thermal_inconsistencies(
        dfs.get("tmin"), dfs.get("tmean"), dfs.get("tmax")
    )

    # Auditoría estadística sobre la variable 'var' (cargar archivo QC de la variable)
    p_var = Path(folder_out) / build_filename(var, periodo, estacion, "qc")
    if p_var.exists():
        df_qc = read_series(str(p_var))
    else:
        df_qc = None

    estad_report = {"outliers": [], "kept": [], "bounds": None}
    if df_qc is not None:
        serie_valida = df_qc[df_qc["valor"] != -99]["valor"]
        bounds = compute_bounds(serie_valida, lower_p=lower_p, upper_p=upper_p, k=k)
        estat = detect_outliers(df_qc, bounds)
        estad_report["bounds"] = bounds
        # load changes to detect maintained
        try:
            changes = json.loads(
                Path(folder_out, "changes_applied.json").read_text(encoding="utf-8")
            )
            changed_dates = {
                entry["fecha"]: entry for entry in changes.get("single_changes", [])
            }
        except Exception:
            changed_dates = {}

        for idx, val in estat:
            fecha = df_qc.loc[idx, "fecha"].strftime("%Y-%m-%d")
            if fecha in changed_dates:
                estad_report["kept"].append(
                    {
                        "fecha": fecha,
                        "valor": float(val),
                        "accion": changed_dates[fecha].get("accion"),
                    }
                )
            else:
                estad_report["outliers"].append({"fecha": fecha, "valor": float(val)})
    # Consolidar informe
    informe = {"termicas": inconsistencias, "estadistico": estad_report}
    # Mostrar resumen
    print("\n===== INFORME DE AUDITORÍA =====")
    print(f'Inconsistencias térmicas encontradas: {len(informe["termicas"]) }')
    for it in informe["termicas"][:5]:
        print(f" - {it['fecha'].strftime('%Y-%m-%d')} → {it['tipo']}")
    print(
        f"Outliers estadísticos (no validados): {len(informe['estadistico']['outliers'])}"
    )
    for o in informe["estadistico"]["outliers"][:5]:
        print(f" - {o['fecha']} → {o['valor']}")
    print("===== FIN INFORME =====\n")

    # Ofrecer corregir ahora si hay problemas
    if informe["termicas"] or informe["estadistico"]["outliers"]:
        resp = (
            input("¿Desea corregir estas inconsistencias ahora? (c)orregir / (m)enu: ")
            .strip()
            .lower()
        )
        if resp == "c":
            print("Entrando a revisión parcial...")
            # Call process_file which will load existing QC via load_triplet and allow corrections
            process_file(
                var,
                periodo,
                estacion,
                folder_in,
                folder_out,
                lower_p=lower_p,
                upper_p=upper_p,
                k=k,
                ventana=7,
                ask_user=input,
            )
    else:
        print("🎉 QC APROBADO: No se encontraron inconsistencias no validadas.")
