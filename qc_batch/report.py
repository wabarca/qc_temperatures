#!/usr/bin/env python3
"""
report.py

Genera un informe PDF del proceso de QC para una variable y estación.

Contenido:
 - Portada
 - Tabla de cambios
 - Gráfica comparativa
 - Estadísticas del QC
 - Gráficas de contexto (opcionales)
"""

from pathlib import Path
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import datetime


# ---------------------------------------------------------------------
# Página de portada
# ---------------------------------------------------------------------
def _pagina_portada(pdf, var, periodo, estacion):
    fig = plt.figure(figsize=(8.5, 11))
    plt.axis("off")

    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    texto = (
        f"INFORME DE CONTROL DE CALIDAD\n\n"
        f"Variable: {var.upper()}\n"
        f"Estación: {estacion}\n"
        f"Periodo: {periodo}\n\n"
        f"Generado el: {fecha}"
    )

    plt.text(0.5, 0.5, texto, ha="center", va="center", fontsize=20, fontweight="bold")
    pdf.savefig(fig)
    plt.close(fig)


# ---------------------------------------------------------------------
# Tabla de cambios
# ---------------------------------------------------------------------
def _pagina_tabla_cambios(pdf, df_changes):
    fig, ax = plt.subplots(figsize=(8.5, 11))
    ax.axis("off")

    ax.set_title("Resumen de cambios aplicados", fontsize=16, fontweight="bold", pad=20)

    # Mostrar solo si hay contenido
    if df_changes.empty:
        ax.text(
            0.5, 0.5, "No se aplicaron cambios.", ha="center", va="center", fontsize=14
        )
    else:
        tabla = ax.table(
            cellText=df_changes.values,
            colLabels=df_changes.columns,
            loc="center",
            cellLoc="center",
        )
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(10)
        tabla.scale(1.1, 1.3)

    pdf.savefig(fig)
    plt.close(fig)


# ---------------------------------------------------------------------
# Insertar gráfica comparativa (PNG)
# ---------------------------------------------------------------------
def _pagina_grafica_comparativa(pdf, path_png):
    fig = plt.figure(figsize=(8.5, 11))
    plt.axis("off")

    if Path(path_png).exists():
        img = plt.imread(path_png)
        plt.imshow(img)
        plt.axis("off")
    else:
        plt.text(
            0.5,
            0.5,
            f"No se encontró {path_png}",
            ha="center",
            va="center",
            fontsize=14,
        )

    pdf.savefig(fig)
    plt.close(fig)


# ---------------------------------------------------------------------
# Página de estadísticas
# ---------------------------------------------------------------------
def _pagina_estadisticas(pdf, df_changes):
    fig, ax = plt.subplots(figsize=(8.5, 11))
    ax.axis("off")

    ax.set_title(
        "Estadísticas del proceso de QC", fontsize=16, fontweight="bold", pad=20
    )

    if df_changes.empty:
        ax.text(
            0.5, 0.5, "No se realizaron cambios.", ha="center", va="center", fontsize=14
        )
    else:
        total = len(df_changes)
        term = (df_changes["tipo_cambio"] == "termico").sum()
        estad = (df_changes["tipo_cambio"] == "estadistico").sum()
        ambos = (df_changes["tipo_cambio"] == "ambos").sum()

        texto = (
            f"Total de fechas evaluadas: {total}\n\n"
            f"• Cambios termodinámicos: {term}\n"
            f"• Cambios estadísticos: {estad}\n"
            f"• Cambios mixtos (ambos): {ambos}\n"
        )

        ax.text(0.1, 0.7, texto, fontsize=13, va="top")

    pdf.savefig(fig)
    plt.close(fig)


# ---------------------------------------------------------------------
# Figuras de contexto (una por página)
# ---------------------------------------------------------------------
def _paginas_contexto(pdf, folder_out, var, periodo, estacion, df_changes):
    """Inserta una página por cada fecha modificada"""
    ctx_dir = Path(folder_out) / "contexto"

    if not ctx_dir.exists():
        return

    for _, row in df_changes.iterrows():
        fecha = row["fecha"]
        tipo = row["tipo_cambio"]

        if tipo == "ninguno":
            continue

        fname = f"contexto_{var}_{estacion}_{fecha.replace('-', '')}.png"
        path_img = ctx_dir / fname

        fig = plt.figure(figsize=(8.5, 11))
        plt.axis("off")

        if path_img.exists():
            img = plt.imread(path_img)
            plt.imshow(img)
            plt.axis("off")
        else:
            plt.text(
                0.5,
                0.5,
                f"No se encontró {path_img}",
                ha="center",
                va="center",
                fontsize=14,
            )

        pdf.savefig(fig)
        plt.close(fig)


# ---------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------
def generar_informe_pdf(
    folder_out: str, var: str, periodo: str, estacion: str, df_changes: pd.DataFrame
):
    """
    Crea el PDF final del QC.
    """
    fname = f"{var}_{periodo}_{estacion}_QC_report.pdf"
    output_path = Path(folder_out) / fname

    png_path = Path(folder_out) / f"{var}_{periodo}_{estacion.upper()}_comparacion.png"

    with PdfPages(output_path) as pdf:
        _pagina_portada(pdf, var, periodo, estacion)
        _pagina_tabla_cambios(pdf, df_changes)
        _pagina_grafica_comparativa(pdf, png_path)
        _pagina_estadisticas(pdf, df_changes)
        _paginas_contexto(pdf, folder_out, var, periodo, estacion, df_changes)

    print(f"📄 Informe PDF generado: {output_path}")
    return str(output_path)
