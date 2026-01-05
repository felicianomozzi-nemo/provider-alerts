# Provider Alerts – Data Quality & Operational Monitoring

Este proyecto es una **demo inicial de Ciencia de Datos aplicada al negocio** para Nemo, orientada a detectar **proveedores con problemas operativos** a partir de datos reales de uso de la plataforma.

El objetivo principal es demostrar el **potencial de explotar datos existentes** para generar alertas accionables, sentando las bases para una futura solución con histórico y análisis avanzado.

---

## 🎯 Objetivo del proyecto

- Analizar datos operativos de proveedores de turismo (hoteles, bedbanks, etc.)
- Detectar:
  - **Problemas técnicos** (fallas en operaciones)
  - **Bajo volumen operativo**
- Generar reportes claros y visuales para consumo no técnico
- Diseñar una solución **simple, escalable y extensible**

Esta primera versión utiliza **umbrales hardcodeados**, con la idea de evolucionar luego a **umbrales dinámicos basados en histórico por proveedor**.

---

## 🧩 Alcance de la versión actual (v1)

✔ Scripts ejecutados manualmente  
✔ Entrada basada en archivos CSV  
✔ Período de análisis configurable (24h, 7 días, etc.)  
✔ Reporte final en Excel con semáforo visual  
✔ Código documentado y linted con Pylint  

🚫 No incluye histórico (por ahora)  
🚫 No incluye automatización ni scheduling  

---

## 📂 Estructura del proyecto

```text
provider-alerts/
│
├── data_transformation.py        # Consolida datos operativos por proveedor
├── provider_alerts.py            # Evalúa alertas y genera reporte Excel
├── .env                          # Configuración de rutas (no versionado)
├── .pylintrc                     # Reglas de calidad de código
├── docs/
│   ├── booking_flow.csv          # Input: flujo de operaciones
│
└── README.md
```
---
## 📥 Datos de entrada

---
### booking_flow.csv

Datos operativos por intento de operación dentro de la plataforma.

Columnas mínimas esperadas:

- `providername`
- `success` (boolean)
- `Time` (no obligatorio para esta versión)

El período de análisis depende del rango de fechas incluido en el CSV exportado
(últimas 24hs, última semana, etc.).

---
## 📤 Outputs generados

---
### 1️⃣ provider_summary.csv

Archivo intermedio con el resumen operativo por proveedor:

- Total de operaciones
- Operaciones exitosas
- Operaciones fallidas
- Tasa de fallas

Este archivo es utilizado como entrada para el script de alertas.

---

### 2️⃣ provider_alerts.xlsx

Reporte final en formato Excel con alertas visuales:

- Estado de fallas técnicas
- Estado de volumen operativo
- Colores tipo semáforo para rápida interpretación

---
## 🚦 Lógica de alertas (v1)

Las alertas se evalúan en dos dimensiones independientes:
fallas técnicas y volumen operativo.

Los umbrales están hardcodeados en esta primera versión y
serán reemplazados en el futuro por valores históricos por proveedor.

---
### 🔧 Fallas técnicas

Las fallas técnicas solo se evalúan si el proveedor tiene
un mínimo de **500 operaciones** en el período analizado.

| Estado        | Condición            |
|--------------|----------------------|
| NORMAL       | < 10% fallas         |
| ATENCIÓN     | 10% – 25% fallas     |
| SEVERIDAD    | > 25% fallas         |
| NO EVALUABLE | < 500 operaciones    |

---
### 📉 Bajo volumen operativo

El volumen operativo se evalúa independientemente del estado técnico.

| Estado   | Operaciones |
|---------|-------------|
| URGENTE | < 100       |
| SEVERO  | 100 – 499   |
| ATENCIÓN| 500 – 1999  |
| NORMAL  | ≥ 2000      |

---
## 🎨 Semáforo visual (Excel)

Los estados de alerta se representan visualmente mediante colores
para facilitar la interpretación por perfiles no técnicos.

- 🟢 Verde → NORMAL
- 🟡 Amarillo → ATENCIÓN
- 🟠 Naranja → SEVERO
- 🔴 Rojo → URGENTE / SEVERIDAD
- ⚪ Gris → NO EVALUABLE

---
## ⚙️ Configuración

El proyecto utiliza un archivo `.env` para definir rutas de archivos
y evitar hardcodear paths en el código.

Ejemplo de `.env`:

```env
BOOKING_FLOW_URL=docs/booking_flow.csv
PROVIDER_SUMMARY_CSV=docs/provider_summary.csv
PROVIDER_ALERTS_EXCEL=provider_alerts.xlsx
```
---
## ▶️ Ejecución

---
### 1️⃣ Consolidar datos operativos

Ejecuta el script de transformación:

```bash
python data_transformation.py
```
---
### 2️⃣ Generar reporte de alertas

Ejecuta el script de alertas:

```bash
python provider_alerts.py
```
Este script genera el archivo provider_alerts.xlsx.

---
## 🧪 Calidad de código

El proyecto utiliza **Pylint** con una configuración adaptada a:

- Scripts de análisis de datos
- Demos de Ciencia de Datos
- Código exploratorio orientado a negocio

Para ejecutar el análisis de calidad:

```bash
pylint *.py
```
---
## 🔮 Roadmap

Posibles evoluciones del proyecto:

- Incorporar histórico por proveedor
- Calcular promedios históricos y desviaciones estándar
- Implementar detección de anomalías
- Reemplazar umbrales hardcodeados por thresholds dinámicos
- Automatizar ejecución diaria
- Integrar con dashboards o sistemas de alertas

---
## 💡 Valor para el negocio

Este proyecto demuestra que:

- Los datos existentes permiten detectar problemas operativos reales
- Es posible generar alertas accionables sin grandes inversiones iniciales
- La Ciencia de Datos puede integrarse de forma incremental
- Los resultados son comprensibles para perfiles no técnicos

La solución prioriza impacto y simplicidad sobre complejidad técnica.

---
## 🏷️ Nota final

Este repositorio representa una **prueba de concepto** orientada a validar
impacto y viabilidad, no una solución productiva final.

La arquitectura fue diseñada pensando en evolución futura,
evitando reescrituras significativas.
