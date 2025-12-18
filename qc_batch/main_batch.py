#!/usr/bin/env python3
"""
main_batch.py

Ejecutor principal del sistema de QC en lote.
Escanea la carpeta de entrada en busca de archivos *_org.csv,
pregunta qué hacer si existen versiones TMP o QC,
y llama al workflow para procesar cada archivo.

Menú igual al estilo clásico del código original:

  (s) Omitir y marcar como COMPLETADO
  (n) Revisar nuevamente desde cero
  (p) Posponer — omitir solo en esta ejecución
  (r) Reanudar desde versión temporal (TMP)

"""

import argparse
import os
from pathlib import Path
import re
from qc_batch.io_manager import parse_filename, build_filename
from qc_batch.workflow import process_file
import pandas as pd


# ---------------------------------------------------------------------
# Buscar archivos *_org.csv en la carpeta de entrada
# ---------------------------------------------------------------------
def existe_estado_triplete(folder_out, estacion):
    from pathlib import Path

    estacion = estacion.upper()
    folder_out = Path(folder_out)

    patrones = [
        f"tmin_*_{estacion}_tmp.csv",
        f"tmean_*_{estacion}_tmp.csv",
        f"tmax_*_{estacion}_tmp.csv",
        f"tmin_*_{estacion}_QC.csv",
        f"tmean_*_{estacion}_QC.csv",
        f"tmax_*_{estacion}_QC.csv",
    ]

    for patron in patrones:
        if list(folder_out.glob(patron)):
            return True

    return False


def buscar_archivos_org(folder_in: str):
    folder = Path(folder_in)
    archivos = []
    # Buscar archivos con _org.csv pero también aceptar archivos sin sufijo
    for f in sorted(folder.glob("*.csv")):
        info = parse_filename(f.name)
        if info is None:
            continue
        # aceptar solo aquellos que son ORG o que no tienen suffix (compatibilidad)
        if info.get("suffix") and info.get("suffix").lower() != "org":
            continue
        archivos.append(
            {
                "path": f,
                "var": info["var"],
                "periodo": info["periodo"],
                "estacion": info["estacion"],
                "estacion": info["estacion"].upper(),
                "periodo": info["periodo"].strip(),
            }
        )
    return archivos


# ---------------------------------------------------------------------
# Menú
# ---------------------------------------------------------------------
def menu_interactivo(archivo, existe_tmp, existe_qc, folder_out):
    """
    Menú interactivo mejorado para archivos con QC o TMP previos.
    Compatible con flujos QC y TMP.
    """

    print("\n===========================================")
    print(f"📄 Procesando archivo: {archivo}")
    print("===========================================\n")

    # ------------------------------------------------------------------
    # CASO 1: Existe una versión QC definitiva previa
    # ------------------------------------------------------------------
    if existe_qc:
        print("⚠️  Se encontró una versión **QC** previa para este archivo.\n")

        print("Opciones disponibles:")
        print("   (v) 👀 Ver el archivo QC (muestra tabla y gráfica comparativa)")
        print("       → Inspeccionar antes de tomar una decisión.\n")

        print("   (a) 🔎 Auditar QC (térmico + estadístico)")
        print("       → Revisa el QC sin modificarlo y muestra un informe.\n")

        print("   (p) 🛠 Revisar QC parcialmente (corregir inconsistencias existentes)")
        print(
            "       → Cargar el QC y permitir correcciones puntuales (no desde cero).\n"
        )

        print("   (r) 🔁 Revisar nuevamente desde cero")
        print("       → Ignora el QC previo y vuelve a cargar la versión ORG.\n")

        print("   (s) ✔  Mantener QC como definitivo y omitir")
        print("       → El QC previo se considera válido.\n")

        while True:
            resp = input("Seleccione una opción: ").strip().lower()

            if resp == "v":
                # Mostrar QC (tabla parcial) y también intentar mostrar la gráfica comparativa
                try:
                    path_qc = Path(folder_out) / archivo.replace("_org.csv", "_QC.csv")
                    df = pd.read_csv(path_qc)
                    print(df.head())
                except:
                    print("⚠️ No se pudo mostrar el QC (tabla).\n")

                # Intentar mostrar figura comparativa si existe
                try:
                    # la figura se guarda con estacion.upper()
                    parsed = parse_filename(archivo)
                    var = parsed["var"]
                    periodo = parsed["periodo"]
                    estacion = parsed["estacion"]
                    fname_png = f"{var}_{periodo}_{estacion.upper()}_comparacion.png"
                    p = Path(folder_out) / fname_png

                    if p.exists():
                        print(f"🖼 Mostrando gráfica comparativa: {p.name}")
                        from qc_batch.visualization import plot_image_preview

                        try:
                            plot_image_preview(str(p))
                        except Exception:
                            pass
                    else:
                        print("ℹ️ No se encontró la gráfica comparativa.\n")
                except Exception:
                    pass

                continue  # volver a mostrar menú para decidir

            elif resp in ("a", "p", "r", "s"):
                return resp

            print("❌ Opción inválida.\n")

    # ------------------------------------------------------------------
    # CASO 2: Existe TMP pero NO QC
    # ------------------------------------------------------------------
    if existe_tmp and not existe_qc:
        print("⚠️  Se encontró una versión **TEMPORAL (TMP)** para este archivo.\n")

        print("Opciones disponibles:")
        print("   (r) 🔄 Reanudar desde la versión TMP")
        print("       → Continúa desde donde quedó el proceso.\n")

        print("   (n) 🧹 Revisar nuevamente desde cero")
        print("       → Elimina TMP y carga la versión ORG.\n")

        print("   (s) ✔  Marcar como COMPLETADO y omitir")
        print("       → Solo si ya revisó manualmente y está correcto.\n")

        print("   (p) ⏭  Posponer solo esta ejecución\n")

        while True:
            resp = input("Seleccione una opción: ").strip().lower()

            if resp in ("r", "n", "s", "p"):
                return resp

            print("❌ Opción inválida.\n")

    # ------------------------------------------------------------------
    # CASO 3: No existía QC ni TMP → ORG limpio
    # ------------------------------------------------------------------
    return "n"


# ---------------------------------------------------------------------
# Ejecución principal por archivo
# ---------------------------------------------------------------------
def procesar_archivo(entry, folder_in, folder_out, ventana, lower_p, upper_p, k):
    var = entry["var"]
    periodo = entry["periodo"]
    estacion = entry["estacion"]

    # Detectar si existen tmp o qc
    fname_tmp = build_filename(var, periodo, estacion, "tmp")
    fname_qc = build_filename(var, periodo, estacion, "qc")

    existe_tmp = Path(folder_out, fname_tmp).exists()
    existe_qc = Path(folder_out, fname_qc).exists()

    # Mostrar menú clásico y pedir acción
    accion = menu_interactivo(
        archivo=entry["path"].name,
        existe_tmp=existe_tmp,
        existe_qc=existe_qc,
        folder_out=folder_out,
    )

    # Procesar según acción
    if accion == "s":
        # marcar como completado sin procesar
        print(f"✔ Marcado como COMPLETADO: {entry['path'].name}\n")
        return

    if accion == "p":
        # Si existe QC, 'p' quiere decir "Revisar QC parcialmente".
        if existe_qc:
            print(f"🛠 Revisando QC parcialmente para: {entry['path'].name}\n")
            process_file(
                var=var,
                periodo=periodo,
                estacion=estacion,
                folder_in=folder_in,
                folder_out=folder_out,
                start_from="qc",
                ask_user=input,
            )
            return
        # Si no existe QC (y el menú devolvió 'p' en el caso TMP), posponer
        else:
            print(f"⏭ Omitido en esta ejecución: {entry['path'].name}\n")
            return

    if accion == "n":
        # hay_estado = existe_estado_triplete(folder_out, estacion)

        # start_mode = "auto" if hay_estado else "org"

        # ===================== LOG CLAVE =====================
        print(
            f"[BATCH] Estación {estacion} | Periodo {periodo} | "
            "Acción usuario: DESDE CERO → forzando ORG"
        )
        # =====================================================

        # print(
        #     f"🔄 Procesando {'con estado previo' if hay_estado else 'desde ORG limpio'}: "
        #     f"{entry['path'].name}\n"
        # )

        process_file(
            var=var,
            periodo=periodo,
            estacion=estacion,
            folder_in=folder_in,
            folder_out=folder_out,
            start_from="org",
            lower_p=lower_p,
            upper_p=upper_p,
            k=k,
            ventana=ventana,
            ask_user=input,
        )
        return

    if accion == "a":
        print("\n🔎 Ejecutando auditoría del QC...\n")
        from qc_batch.workflow import auditar_qc

        auditar_qc(var, periodo, estacion, folder_in, folder_out)
        return

    if accion == "r":
        # Revisar desde cero SI el usuario lo pidió expresamente,
        # ignorando QC y TMP por completo.
        print(f"🔄 Revisando desde cero (ignorando QC y TMP): {entry['path'].name}\n")

        process_file(
            var=var,
            periodo=periodo,
            estacion=estacion,
            folder_in=folder_in,
            folder_out=folder_out,
            start_from="auto",
            lower_p=lower_p,
            upper_p=upper_p,
            k=k,
            ventana=ventana,
            ask_user=input,
        )
        return


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Ejecutor en lote del control de calidad de series climáticas."
    )

    parser.add_argument(
        "-i", "--input", required=True, help="Carpeta de entrada que contiene *_org.csv"
    )

    parser.add_argument(
        "-o", "--output", required=True, help="Carpeta de salida para guardar tmp y qc"
    )

    parser.add_argument(
        "--ventana",
        type=int,
        default=7,
        help="Días hacia atrás y adelante para la gráfica de contexto (default: 7)",
    )

    parser.add_argument(
        "--lower-p",
        type=float,
        default=0.1,
        help="Percentil inferior para control estadístico (default: 0.1)",
    )

    parser.add_argument(
        "--upper-p",
        type=float,
        default=0.9,
        help="Percentil superior para control estadístico (default: 0.9)",
    )

    parser.add_argument(
        "-k", type=float, default=1.5, help="Multiplicador del IQR (default: 1.5)"
    )

    args = parser.parse_args()

    folder_in = args.input
    folder_out = args.output
    ventana = args.ventana

    lower_p = args.lower_p
    upper_p = args.upper_p
    k = args.k

    # Buscar archivos ORG
    entradas = buscar_archivos_org(folder_in)

    if not entradas:
        print("❌ No se encontraron archivos *_org.csv en la carpeta de entrada.")
        return

    print(f"\n🔍 Detectados {len(entradas)} archivos para procesar.\n")

    # Procesar cada archivo
    for entry in entradas:
        var = entry["var"].lower()

        # OMITIR variables no térmicas
        if var not in ("tmin", "tmean", "tmax"):
            print(f"⏭ Omitiendo variable no térmica: {var}")
            continue
        procesar_archivo(entry, folder_in, folder_out, ventana, lower_p, upper_p, k)


if __name__ == "__main__":
    main()
