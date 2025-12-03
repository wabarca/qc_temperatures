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
    write_tmp,
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
):
    """
    Procesa una variable para una estación específica.
    """

    if ask_user is None:
        ask_user = input

    # Excluir variables NO térmicas (solo pr)
    if var.lower() not in ("tmax", "tmean", "tmin"):
        print(f"⏭ Omitiendo variable no térmica: {var}")
        return

    # Archivo base
    base_info = find_candidate_file(folder_in, folder_out, var, periodo, estacion)
    status = base_info["status"]
    path_base = base_info["path"]

    if status is None:
        print(f"No se encontró archivo para {var}_{periodo}_{estacion}")
        return

    print(f"[{var.upper()}] Archivo base detectado: {status.upper()} → {path_base}")

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

    # =========================================================
    #  CONTROL TERMODINÁMICO (BUCLE DINÁMICO CORREGIDO)
    # =========================================================
    dfs_trip = load_triplet(folder_in, folder_out, periodo, estacion)

    # Detectar inconsistencias iniciales
    inconsist = detect_thermal_inconsistencies(
        dfs_trip["tmin"], dfs_trip["tmean"], dfs_trip["tmax"]
    )

    # Inicializar resumen térmico
    resumen_termico = []

    total_inconsist = len(inconsist)
    print(f"🌡 Se detectaron {total_inconsist} inconsistencias térmicas iniciales.")
    corregidas = 0

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

        # Comparar solo si el usuario realmente lo desea, UNA sola vez por inconsistencia
        if ask_user(
            "¿Desea comparar con otra estación para el mismo día? (s/n): "
        ).strip().lower() in ("s", "y"):
            compare_with_other_station(
                var, periodo, estacion, fecha, folder_in, folder_out, ventana, ask_user
            )

        # -----------------------------------------------------------
        # Merge robusto del triplete (corrige arrastre incorrecto)
        # -----------------------------------------------------------
        trip = (
            dfs_trip["tmin"][["fecha", "valor"]]
            .rename(columns={"valor": "tmin"})
            .merge(
                dfs_trip["tmean"][["fecha", "valor"]].rename(
                    columns={"valor": "tmean"}
                ),
                on="fecha",
                how="outer",
            )
            .merge(
                dfs_trip["tmax"][["fecha", "valor"]].rename(columns={"valor": "tmax"}),
                on="fecha",
                how="outer",
            )
        )

        # Asegurar orden por fecha
        trip = trip.sort_values("fecha")

        # Extraer fila exacta para la inconsistencia
        vals = trip.loc[trip["fecha"] == fecha]

        # Validación: si falta valor se toma como -99 explícito (no mezcla datos viejos)
        tmin_val = float(vals["tmin"].fillna(-99).iloc[0])
        tmean_val = float(vals["tmean"].fillna(-99).iloc[0])
        tmax_val = float(vals["tmax"].fillna(-99).iloc[0])

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

        # --------- guardar TMP después de la acción ---------
        for v_local, df_local in dfs_trip.items():
            fname_tmp = build_filename(v_local, periodo, estacion, "tmp")
            ruta_tmp = os.path.join(folder_out, fname_tmp)

            df_out = df_local.copy()
            df_out[df_out.columns[0]] = df_out[df_out.columns[0]].dt.strftime("%Y%m%d")
            df_out.to_csv(ruta_tmp, index=False)

        print(f"💾 TMP guardado para {fecha_str}.")

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
    write_triplet_tmp(dfs_trip, folder_out, periodo, estacion)

    print("\n===== RESUMEN DE CORRECCIONES TÉRMICAS =====")
    for item in resumen_termico:
        print(f" • {item['fecha']}  →  {item['tipo']}  → acción '{item['accion']}'")
    print("===========================================\n")

    # DF base de la variable
    dfs_trip = load_triplet(folder_in, folder_out, periodo, estacion)
    df_base = dfs_trip[var]

    # =========================================================
    # CONTROL ESTADÍSTICO
    # =========================================================
    serie_valida = df_base[df_base["valor"] != -99]["valor"]
    bounds = compute_bounds(serie_valida, lower_p=lower_p, upper_p=upper_p, k=k)
    outliers = detect_outliers(df_base, bounds)

    total_outliers = len(outliers)
    corregidos_est = 0
    print(f"📊 Se detectaron {total_outliers} outliers estadísticos.")
    resumen_outliers = []

    for idx, val in outliers:
        fecha = df_base.loc[idx, "fecha"]

        print(f"\nOutlier estadístico en {fecha.date()} → {val}")

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

        # Comparación por ID
        compare_with_other_station(
            var, periodo, estacion, fecha, folder_in, folder_out, ventana, ask_user
        )

        # ==================================================
        # MENÚ ESTADÍSTICO MEJORADO
        # ==================================================

        # Obtener límites estadísticos
        p_low = bounds["p_low"]
        p_high = bounds["p_high"]
        iqr = bounds["iqr"]

        valor = val
        fecha_str = fecha.strftime("%Y-%m-%d")

        # Sugerencia automática
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

        print("Acciones disponibles:")
        print("   (s) ❌ Sustituir valor por -99")
        print("   (m) 👍 Mantener valor original")
        print("   (n) ✏  Ingresar nuevo valor manualmente")
        print("   (i) 🔄 Intercambiar con tmin/tmax si aplica")
        print("   (p) ⏭  Pasar sin hacer cambios\n")

        # Capturar acción
        action = ask_user("Seleccione acción: ").strip().lower()

        # ENTER = aceptar sugerencia
        if action == "" and sug:
            action = sug

        # Validar
        while action not in ("s", "m", "n", "i", "p"):
            print("❌ Acción inválida.")
            action = ask_user("Seleccione acción: ").strip().lower()
            if action == "" and sug:
                action = sug

        # Aplicar acción
        if action == "n":
            nuevo = float(ask_user(f"Nuevo valor para {fecha_str}: ").strip())
            df_base = apply_statistical_decision(df_base, idx, "n", nuevo)
            corregidos_est += 1
            print(f"✔ Outlier corregido. Progreso: {corregidos_est}/{total_outliers}\n")

            resumen_outliers.append(
                {"fecha": fecha_str, "valor": val, "accion": action}
            )

        else:
            df_base = apply_statistical_decision(df_base, idx, action)
            corregidos_est += 1
            print(f"✔ Outlier corregido. Progreso: {corregidos_est}/{total_outliers}\n")

            resumen_outliers.append(
                {"fecha": fecha_str, "valor": val, "accion": action}
            )

            # ✔ Cerrar todas las ventanas abiertas
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
    path_qc = write_qc(df_base, folder_out, var, periodo, estacion)
    print(f"\n✔ Archivo QC generado: {path_qc}")

    mark_completed(folder_out, Path(path_qc).name)

    # =========================================================
    # ARCHIVO DE CAMBIOS
    # =========================================================
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
