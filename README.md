# Provider Alerts – Data Quality & Operational Monitoring

Este proyecto es una **demo avanzada de Ciencia de Datos aplicada al negocio** para Nemo, orientada a detectar **proveedores con problemas operativos** a partir de datos reales extraídos directamente desde Kibana.

El objetivo es demostrar el **potencial de explotar datos existentes** para generar alertas accionables, incorporando:

- Extracción automatizada desde Elasticsearch/Kibana
- Transformación y consolidación de métricas
- Evaluación de alertas operativas
- Reporte visual en Excel
- Base preparada para evolución hacia análisis histórico inteligente

---

# 🎯 Objetivo del proyecto

- Conectarse a Kibana (APM + Business)
- Extraer eventos reales de Booking Flow
- Consolidar métricas por proveedor
- Detectar:
  - Problemas técnicos (alta tasa de fallas)
  - Bajo volumen operativo
- Generar reportes claros y visuales para perfiles no técnicos
- Diseñar una solución escalable y extensible

---

# 🧩 Alcance de la versión actual (v2)

✔ Conexión directa a Kibana  
✔ Extracción paginada usando `search_after`  
✔ Modo estándar por rango de tiempo  
✔ Modo histórico incremental  
✔ Consolidación automática por proveedor  
✔ Reporte final en Excel con semáforo visual  
✔ Configuración desacoplada vía `.env`  
✔ Código documentado y estructurado  

🚫 Aún no incluye detección estadística avanzada  
🚫 Aún no incluye automatización programada  

---

# 📂 Estructura del proyecto

provider-alerts/
│
├── data_extraction.py            # Conexión a Kibana + extracción de datos
├── data_transformation.py        # Consolidación de métricas por proveedor
├── providers_alerts.py           # Evaluación de alertas + Excel final
├── .env                          # Variables de entorno (no versionado)
├── .pylintrc                     # Configuración de calidad de código
├── README.md

---

# 🔄 Flujo completo del sistema

1️⃣ Extracción desde Kibana  
2️⃣ Generación de CSV crudo  
3️⃣ Transformación y agregación por proveedor  
4️⃣ Evaluación de alertas  
5️⃣ Exportación a Excel con formato visual  

---

# 📡 Extracción de datos (data_extraction.py)

Este script:

- Verifica conectividad y autenticación contra:
  - Kibana APM
  - Kibana Business
- Extrae datos usando paginación `search_after`
- Normaliza nombres
- Exporta CSV crudo

## Modos de ejecución

### 🔹 Modo estándar

Extrae datos para un rango de tiempo específico:

```
python data_extraction.py now-24h
python data_extraction.py now-7d
python data_extraction.py now-30d
```

Si no se pasa parámetro:

```
python data_extraction.py
```

Por defecto usa:

```
now-10h
```

---

### 🔹 Modo histórico incremental

```
python data_extraction.py historic
```

Comportamiento:

- Si no existe archivo histórico → descarga últimos 10 años
- Si existe → detecta último timestamp y trae solo nuevos registros
- Actualiza el CSV histórico acumulado

Esto permite construir una base de datos evolutiva sin reprocesar todo cada vez.

---

# 📊 Transformación de datos (data_transformation.py)

Este script:

- Lee el CSV crudo exportado
- Agrupa por proveedor
- Calcula métricas operativas

## Métricas generadas

- total_operations
- successful_operations
- failed_operations
- failure_rate

Output:

```
SUMMARY_OUTPUT_CSV_URL
```

---

# 🚦 Evaluación de alertas (providers_alerts.py)

Este script:

- Lee el resumen por proveedor
- Evalúa dos tipos de alertas
- Genera Excel formateado con colores

## 🔧 Alerta técnica (failure_rate)

Solo se evalúa si el proveedor tiene ≥ 500 operaciones.

| Estado           | Condición |
|------------------|-----------|
| NORMAL           | < 10%     |
| CONCERN          | 10%–25%   |
| SEVERE           | > 25%     |
| CAN'T EVALUATE   | < 500 ops |

---

## 📉 Alerta de volumen

| Estado   | Operaciones |
|----------|-------------|
| URGENT   | < 100       |
| SEVERE   | 100–499     |
| CONCERN  | 500–1999    |
| NORMAL   | ≥ 2000      |

---

# 🎨 Semáforo visual en Excel

Las columnas:

- failure_alert
- volume_alert

Se colorean automáticamente usando `openpyxl`.

Colores:

- Verde → NORMAL
- Amarillo → CONCERN
- Rojo → SEVERE
- Bordó → URGENT
- Gris → CAN'T EVALUATE

Output final:

```
PROVIDER_ALERTS_EXCEL
```

---

# ▶️ Ejecución end-to-end

Paso 1 – Extraer datos:
```
python data_extraction.py now-24h
```

Paso 2 – Transformar:
```
python data_transformation.py
```
Paso 3 – Generar alertas:
```
python providers_alerts.py
```
Resultado final:
```
provider_alerts.xlsx
```
---

# 🧪 Calidad de código

El proyecto utiliza:

- Pylint
- Estructura modular
- Funciones desacopladas
- Manejo defensivo de errores
- Logs descriptivos

Para ejecutar análisis:
```
pylint *.py
```
---

# 🔮 Roadmap

Evoluciones previstas:

- Incorporar métricas históricas por proveedor
- Calcular baseline dinámico
- Detección automática de anomalías
- Thresholds adaptativos
- Automatización diaria (cron / scheduler)
- Integración con dashboards
- Sistema de notificaciones (Slack / Email)

---

# 💡 Valor para el negocio

Este proyecto demuestra que:

- Los datos operativos actuales ya contienen señales accionables
- Es posible detectar problemas técnicos antes de que escalen
- Se pueden identificar proveedores con caída de volumen
- La Ciencia de Datos puede integrarse incrementalmente
- Se puede generar impacto sin infraestructura compleja

---

# 🏷️ Nota final

Este repositorio representa una **prueba de concepto avanzada**.

La arquitectura fue diseñada pensando en:

- Escalabilidad
- Evolución hacia modelos históricos
- Posible automatización productiva futura
- Integración con sistemas internos

Es una base sólida para evolucionar hacia un sistema real de monitoreo inteligente de proveedores.
