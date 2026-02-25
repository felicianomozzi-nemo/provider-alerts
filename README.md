# Data Alerts – Operational Monitoring & Intelligent Alerting

Proyecto de Ciencia de Datos orientado a la detección temprana de desvíos operativos en el ecosistema de Booking Flow.

Data Alerts permite:

- Extraer datos reales desde Kibana (Business + APM)
- Construir métricas operativas consolidadas
- Comparar contra histórico acumulado
- Detectar desvíos técnicos y de volumen
- Generar reportes visuales en Excel con semáforo automático

El objetivo es demostrar cómo explotar datos existentes para generar alertas accionables sin necesidad de infraestructura compleja.

------------------------------------------------------------
🎯 Objetivo del Proyecto
------------------------------------------------------------

1. Conectarse a Kibana y validar conectividad
2. Extraer eventos reales del Booking Flow
3. Construir dataset histórico incremental
4. Consolidar métricas por entidad
5. Comparar período actual vs histórico
6. Detectar desvíos relevantes
7. Generar reporte visual para toma de decisiones

------------------------------------------------------------
📦 Alcance de la Versión Actual
------------------------------------------------------------

✔ Conexión validada a Kibana (APM + Business)  
✔ Extracción paginada con search_after  
✔ Modo estándar por rango dinámico de tiempo  
✔ Modo histórico incremental (acumulativo)  
✔ Consolidación flexible por múltiples dimensiones  
✔ Comparación contra baseline histórico  
✔ Clasificación de alertas por severidad  
✔ Exportación a Excel con formato condicional  
✔ Configuración desacoplada vía .env  

No incluye todavía:
- Modelos estadísticos avanzados
- Machine Learning
- Automatización programada (cron / scheduler)
- Notificaciones automáticas

------------------------------------------------------------
📂 Estructura del Proyecto
------------------------------------------------------------

data-alerts/
│
├── data_extraction.py
├── data_transformation.py
├── generate_alerts.py
├── .env
├── README.md

------------------------------------------------------------
🔄 Flujo Completo del Sistema
------------------------------------------------------------

1) Extracción desde Kibana  
2) Generación de CSV crudo  
3) Transformación y agregación  
4) Construcción de baseline histórico  
5) Evaluación de alertas  
6) Exportación a Excel formateado  

------------------------------------------------------------
📡 1) data_extraction.py
------------------------------------------------------------

Responsabilidades:

- Verifica conectividad vía socket
- Verifica autenticación contra /api/status
- Extrae datos desde Elasticsearch usando proxy de Kibana
- Implementa paginación mediante search_after
- Normaliza nombres
- Exporta CSV crudo
- Permite modo estándar o histórico incremental

------------------------------------------------------------
Modos de Ejecución
------------------------------------------------------------

🔹 Modo estándar

python data_extraction.py now-24h
python data_extraction.py now-7d
python data_extraction.py now-30d

Si no se pasa parámetro:

python data_extraction.py

Por defecto utiliza:
now-10h

El archivo se guarda como:
CURRENT_DATA + <time_range>_data.csv

------------------------------------------------------------

🔹 Modo histórico incremental

python data_extraction.py historic

Comportamiento:

- Si no existe archivo histórico → descarga últimos 10 años
- Si existe → detecta último @timestamp
- Trae únicamente nuevos registros
- Actualiza el CSV acumulado

Esto permite construir una base histórica sin reprocesar todo cada vez.

------------------------------------------------------------
📊 2) data_transformation.py
------------------------------------------------------------

Responsabilidades:

- Lee CSV crudo (actual o histórico)
- Agrupa por columna configurable
- Calcula métricas operativas
- Exporta resumen consolidado

------------------------------------------------------------
Columnas soportadas para agrupación
------------------------------------------------------------

- provider_name
- hotel_name
- client_name
- destination_name

------------------------------------------------------------
Métricas generadas
------------------------------------------------------------

- total_operations
- successful_operations
- failed_operations
- period_days
- daily_avg_operations
- failure_rate

failure_rate = failed_operations / total_operations

daily_avg_operations = total_operations / period_days

------------------------------------------------------------
Modo estándar
------------------------------------------------------------

python data_transformation.py provider_name now-7d

Genera:
SUMMARY_DIR + provider_name_now-7d_summary.csv

------------------------------------------------------------
Modo histórico
------------------------------------------------------------

python data_transformation.py provider_name historic

Genera:
HISTORIC_SUMMARY_DIR + provider_name_summary.csv

------------------------------------------------------------
🚦 3) generate_alerts.py
------------------------------------------------------------

Responsabilidades:

- Lee resumen actual
- Lee resumen histórico
- Une ambos datasets
- Calcula desvíos relativos
- Clasifica severidad
- Exporta Excel formateado

------------------------------------------------------------
📉 Lógica de Alertas
------------------------------------------------------------

Se evalúan dos dimensiones:

1) Desvío de Failure Rate
2) Desvío de Volumen Operativo

------------------------------------------------------------
🛠️ Desvío de Failure Rate
------------------------------------------------------------

failure_deviation =
(current_failure_rate - historic_failure_rate)
/
historic_failure_rate

Clasificación:

<= 25%        → NORMAL
<= 50%        → CONCERN
<= 75%        → SEVERE
> 75%         → URGENT
NaN / inválido → CAN'T EVALUATE

------------------------------------------------------------
📊 Desvío de Volumen
------------------------------------------------------------

volume_deviation =
(current_daily_avg - historic_daily_avg)
/
historic_daily_avg

Clasificación:

>= -25%        → NORMAL
>= -50%        → CONCERN
>= -75%        → SEVERE
< -75%         → URGENT
NaN            → CAN'T EVALUATE

También se calcula:

- volume_difference_absolute

------------------------------------------------------------
🎨 Semáforo Visual en Excel
------------------------------------------------------------

Las columnas:

- Alerta Fallos
- Alerta Volumen

Se colorean automáticamente usando openpyxl.

Colores:

NORMAL → Verde  
CONCERN → Amarillo  
SEVERE → Rojo  
URGENT → Bordó  
CAN'T EVALUATE → Gris  

Output:

OUTPUT_DIR + <group_by_column>_<time_range>.xlsx

------------------------------------------------------------
▶️ Ejecución End-to-End
------------------------------------------------------------

1️⃣ Extraer período actual

python data_extraction.py now-7d

2️⃣ Actualizar histórico (si corresponde)

python data_extraction.py historic

3️⃣ Generar resumen período actual

python data_transformation.py provider_name now-7d

4️⃣ Generar resumen histórico

python data_transformation.py provider_name historic

5️⃣ Generar reporte de alertas

python generate_alerts.py provider_name now-7d

Resultado final:
Archivo Excel con alertas clasificadas y coloreadas.

------------------------------------------------------------
⚙️ Configuración (.env)
------------------------------------------------------------

El proyecto utiliza variables de entorno para:

- URLs de Kibana
- Credenciales
- Índices
- Paths de entrada y salida
- Nombres de archivos resumen
- Directorios históricos

Esto permite desacoplar código de infraestructura.

------------------------------------------------------------
🧪 Calidad de Código
------------------------------------------------------------

- Estructura modular
- Funciones reutilizables
- Manejo defensivo de errores
- Validación de conectividad
- Cálculo incremental histórico
- Normalización de datos

Puede ejecutarse:

pylint *.py

------------------------------------------------------------
🔮 Roadmap de Evolución
------------------------------------------------------------

- Baseline dinámico por ventana móvil
- Detección estadística de anomalías
- Incorporación de métricas adicionales
- Automatización diaria
- Integración con dashboards
- Notificaciones automáticas (Slack / Email)
- API interna de alertas

------------------------------------------------------------
💡 Valor para el Negocio
------------------------------------------------------------

Data Alerts demuestra que:

- Los datos operativos ya contienen señales críticas
- Se pueden detectar desvíos antes de que escalen
- Es posible construir baseline histórico incremental
- Se pueden generar reportes claros para perfiles no técnicos
- La Ciencia de Datos puede integrarse progresivamente

------------------------------------------------------------
🏷️ Nota Final
------------------------------------------------------------

Este repositorio representa una base sólida para evolucionar hacia un sistema de monitoreo inteligente de operaciones.

La arquitectura fue pensada para:

- Escalabilidad
- Evolución analítica
- Bajo costo operativo
- Integración futura con sistemas internos

Data Alerts es el primer paso hacia un monitoreo operacional basado en datos reales.