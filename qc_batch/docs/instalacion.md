# 🛠️ Instalación y configuración del sistema QC-TEMP

## 1. Requisitos

- Python 3.10+
- pandas, numpy, matplotlib, tqdm, pyqt5

## 2. Crear entorno

```bash
conda create -n qc-temperaturas python=3.10
conda activate qc-temperaturas
pip install pandas numpy matplotlib tqdm pyqt5
```

## 3. Estructura de carpetas

```
datasets/
│
├── input/
├── output/
└── logs/
```

## 4. Formato de archivos

```
tmax_1961-2022_S-10_org.csv
tmin_1961-2022_S-10_org.csv
tmean_1970-2022_S-10_org.csv
```
