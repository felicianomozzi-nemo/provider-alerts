# Data Alerts – Monitoreo Operativo y Alertas Inteligentes

Un proyecto de Ciencia de Datos e Ingeniería orientado a la detección temprana de desvíos operativos dentro del ecosistema de Booking Flow.

**Data Alerts** permite:
- Extraer datos de negocio en tiempo real directamente desde Kibana (Business + APM).
- Construir y mantener un baseline histórico incremental.
- Consolidar métricas operativas para detectar fallas técnicas y caídas de volumen.
- Generar reportes visuales en Excel con alertas codificadas por color de forma automática (sistema de semáforo).

Este proyecto demuestra cómo aprovechar datos existentes para generar alertas accionables con una arquitectura limpia, escalable y desacoplada, sin necesidad de infraestructura externa compleja.

---

## 🏗️ Arquitectura y Flujo de Trabajo

El sistema ha sido fuertemente refactorizado siguiendo el **Principio de Responsabilidad Única (SRP)**. El pipeline ahora es orquestado de manera transparente por un módulo central, pasando datos en memoria para reducir las operaciones de lectura/escritura en disco donde sea posible.

1. **`config.py`**: Centraliza todas las variables de entorno y umbrales.
2. **`extractor.py`**: Maneja la conectividad con Kibana, la paginación de la API (`search_after`) y la extracción de datos crudos.
3. **`transformer.py`**: El motor de Data Wrangling. Agrupa los datos actuales, los cruza con los baselines históricos y la información de los proveedores, y calcula las desviaciones matemáticas.
4. **`alerter.py`**: El núcleo de la Lógica de Negocio. Evalúa los datos enriquecidos contra los umbrales predefinidos (`MIN_FAILURE`, `MIN_VOLUME`) y categoriza los datos en subconjuntos accionables.
5. **`reporter.py`**: La capa de Presentación. Formatea columnas, redondea valores y utiliza `openpyxl` para generar un reporte Excel de múltiples hojas codificado por colores.
6. **`main.py`**: El Orquestador. Parsea los argumentos y ejecuta el pipeline completo de principio a fin con un solo comando.

---

## 📦 Características

### ✅ Implementado Actualmente
* **Pipeline Automático End-to-End**: Ejecuta la extracción, transformación y generación de reportes con un solo comando.
* **Modos de Ejecución**:
  * **Modo Estándar**: Extrae datos para una ventana de tiempo específica (ej. `now-7d`) y genera un reporte de alertas actual.
  * **Modo Histórico**: Actualiza incrementalmente el baseline histórico sin reprocesar los últimos 10 años de datos.
* **Tipos de Alertas Evaluadas**:
  * **Tasa de Fallas Técnicas**: Evalúa las operaciones fallidas contra umbrales personalizados dependiendo de configuraciones técnicas (ej. `politics_search`).
  * **Desviación de Volumen**: Detecta caídas abruptas en el volumen operativo diario en comparación con el baseline histórico.
* **Niveles de Severidad**: Categoriza las alertas en `NORMAL` (Verde), `CONCERN` (Amarillo), `SEVERE` (Rojo) y `URGENT` (Rojo Oscuro).
* **Configuración Desacoplada**: Totalmente configurable a través de archivos `.env`.

### 🔮 Roadmap (Próximamente)
* **Agrupación de Entidades Más Amplia**: Aunque la arquitectura del sistema soporta la agrupación por `hotel_name`, `client_name` o `destination_name`, la orquestación por defecto está optimizada actualmente para `provider_name`. El soporte automatizado completo para otras dimensiones está en progreso.
* **Notificaciones Automatizadas**: Integración directa con la API de Slack y SMTP de correo electrónico para enviar alertas automáticamente, evitando la necesidad de revisar el archivo Excel manualmente.
* **Detección Avanzada de Anomalías**: Transición de umbrales estáticos (`-25%`, `10%`) a baselines de media móvil y modelos estadísticos de Machine Learning.
* **Programación con Cron**: Ejecuciones diarias totalmente automatizadas.

---

## 📂 Estructura del Proyecto

```text
data-alerts/
│
├── alerter.py                 # Lógica de negocio y evaluación de umbrales
├── config.py                  # Configuraciones globales y carga de entorno
├── extractor.py               # Extracción de datos de Kibana
├── reporter.py                # Generación y estilos de Excel
├── transformer.py             # Agregación de datos y cruce histórico
├── main.py                    # Orquestador del pipeline
│
├── .env                       # Variables de entorno (NO COMMITEAR)
├── .env.example               # Plantilla para variables de entorno
├── .pylintrc                  # Reglas de configuración del linter
├── requirements.txt           # Dependencias de Python
└── LEEME.md                   # Documentación del proyecto (este archivo)
```

---

## ⚙️ Instalación y Configuración

1. **Clonar el repositorio** a tu máquina local.

2. **Configurar un Entorno Virtual (Recomendado)**:
   Es altamente recomendable ejecutar este proyecto dentro de un entorno virtual para aislar sus dependencias.
   ```bash
   # Crear el entorno virtual
   python -m venv venv

   # Activarlo en Windows:
   venv\Scripts\activate

   # Activarlo en macOS/Linux:
   source venv/bin/activate
   ```

3. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Variables de Entorno**: Crear un archivo `.env` en el directorio raíz basado en la plantilla proporcionada.

### `.env.example`
```env
# =========================
# Entradas y Salidas de Datos
# =========================
HISTORIC_DATA="./historic/historic_data.csv"
CURRENT_DATA="./docs/raw_"
PROVIDER_INFO_URL="./docs/provider_name_info.csv"

SUMMARY_DIR="./summary/"
SUMMARY_ENDING="_operations_summary.csv"
HISTORIC_SUMMARY_DIR="./historic/"

OUTPUT_DIR="./output/"
OUTPUT_ENDING="_alerts.xlsx"

# =========================
# Mapeo de Archivos de Alertas
# =========================
PROVIDERS_SUMMARY_CSV="provider_name_"
HOTELS_SUMMARY_CSV="hotel_name_"
CLIENTS_SUMMARY_CSV="client_name_"
DESTINATIONS_SUMMARY_CSV="destination_name_"

HISTORIC_PROVIDERS_SUMMARY_CSV="historic_provider_name"
HISTORIC_HOTELS_SUMMARY_CSV="historic_hotel_name"
HISTORIC_CLIENTS_SUMMARY_CSV="historic_client_name"
HISTORIC_DESTINATIONS_SUMMARY_CSV="historic_destination_name"

# =========================
# Configuración de Kibana
# =========================
INDEX_URL="bookingflow"

# APM
KIBANA_APM_URL="[https://your-apm-url.com](https://your-apm-url.com)"
KIBANA_APM_USER="your_apm_user"
KIBANA_APM_PASS="your_apm_password"
KIBANA_APM_HEADERS='{"custom-header": "value"}'

# Business
KIBANA_BUSINESS_URL="[https://your-business-url.com](https://your-business-url.com)"
KIBANA_BUSINESS_USER="your_business_user"
KIBANA_BUSINESS_PASS="your_business_password"

# =========================
# Umbrales de Alertas
# =========================
# Porcentaje mínimo de fallas para activar una alerta
MIN_FAILURE=10
# Porcentaje mínimo de caída de volumen para activar una alerta (número negativo)
MIN_VOLUME=-25
```

---

## ▶️ Uso

Gracias al nuevo módulo de orquestación, ya no es necesario ejecutar scripts individualmente. Utiliza `main.py` para dirigir todo el flujo.

### 1. Ejecución Estándar (Por defecto)
Extrae los últimos 7 días de datos, agrupa por proveedor, evalúa las alertas y genera el reporte Excel.
```bash
python main.py
```

### 2. Rango de Tiempo Personalizado
Especifica una expresión de tiempo personalizada de ElasticSearch (ej. `now-14d`, `now-24h`).
```bash
python main.py now-14d
```

### 3. Agrupación Personalizada (Experimental)
Especifica tanto el rango de tiempo como la entidad por la que deseas agrupar.
```bash
python main.py now-10d hotel_name
```

### 4. Actualizar Baseline Histórico
Descarga únicamente los registros nuevos desde la última extracción y actualiza los datos históricos acumulados. *Nota: Este modo omite la generación del reporte Excel.*
```bash
python main.py historic
```