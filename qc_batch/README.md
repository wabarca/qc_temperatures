# 🌡️ Sistema de Control de Calidad para Series de Temperatura (QC-TEMP)
### Tmax · Tmin · Tmean · Precipitación  
**Proyecto institucional — Ministerio de Medio Ambiente (MARN)**

Este repositorio contiene un sistema interactivo para ejecutar **Control de Calidad (QC)** sobre series diarias de temperatura y precipitación:

- **Temperatura Máxima (Tmax)**
- **Temperatura Mínima (Tmin)**
- **Temperatura Media (Tmean)**
- **Precipitación diaria (PR)**

El sistema implementa un flujo robusto basado en criterios operativos del **WMO** e integra:

- ✔ Control termodinámico  
- ✔ Corrección interactiva asistida por gráficos  
- ✔ Sugerencias automáticas (IA de reglas)  
- ✔ Control estadístico por IQR  
- ✔ Fallback para periodos y estaciones incompletas  
- ✔ Guardado de archivos temporales y finales  
- ✔ Bitácora completa de cambios en JSON y CSV  
- ✔ Generación de reportes y gráficos  
- ✔ Cierre automático de ventanas con cada corrección  

---

## 🚀 Instalación rápida

```bash
conda create -n qc-temperaturas python=3.10
conda activate qc-temperaturas
pip install pandas numpy matplotlib pyqt5 tqdm
```

---

## ▶️ Ejecutar el QC

```bash
python main_batch.py --in ./datasets/input --out ./datasets/output
```

---

## 📚 Documentación

- [Instalación detallada](docs/instalacion.md)  
- [Flujo completo del QC](docs/flujo_QC.md)  
- [Ejemplos de uso](docs/ejemplos.md)

---

## 🧱 Estructura del proyecto

```
qc_batch/
│
├── main_batch.py
├── workflow.py
├── thermo_qc.py
├── stat_qc.py
├── visualization.py
├── helpers_compare.py
├── io_manager.py
├── modifications.py
├── report.py
│
└── /datasets/
      ├── input/
      ├── output/
      └── logs/
```
