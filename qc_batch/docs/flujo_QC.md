# 🔄 Flujo completo del Control de Calidad (QC)

## 1. Triplete térmico
Carga de tmax, tmin, tmean con fallback en caso de periodos distintos.

## 2. Reglas termodinámicas
Se evalúan pares válidos únicamente.

## 3. Revisión interactiva
Menú de acciones con sugerencias automáticas y gráficos 2×2.

## 4. Recalculo dinámico
Después de cada acción:
- Se guarda TMP
- Se cierran todas las figuras
- Se recalculan inconsistencias
- Si se corrige → la fecha no vuelve a aparecer

## 5. Control estadístico
Aplicación de IQR para detectar outliers.

## 6. Finalización
Generación de archivos QC, JSON, CSV y figuras.
