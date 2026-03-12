# Data Alerts – Operational Monitoring & Intelligent Alerting

A Data Science and Engineering project aimed at the early detection of operational deviations within the Booking Flow ecosystem. 

**Data Alerts** allows you to:
- Extract real-time business data directly from Kibana (Business + APM).
- Build and maintain an incremental historical baseline.
- Consolidate operational metrics to detect technical failures and volume drops.
- Generate highly visual Excel reports with automated color-coded alerts (Traffic Light system).

This project demonstrates how to leverage existing data to generate actionable alerts with a clean, scalable, and decoupled architecture, without the need for complex external infrastructure.

---

## 🏗️ Architecture & Workflow

The system has been heavily refactored following the **Single Responsibility Principle (SRP)**. The pipeline is now orchestrated seamlessly by a central module, passing data in memory to reduce disk I/O where possible.

1. **`config.py`**: Centralizes all environment variables and thresholds.
2. **`extractor.py`**: Handles Kibana connectivity, API pagination (`search_after`), and raw data extraction.
3. **`transformer.py`**: The Data Wrangling engine. Groups current data, merges it with historical baselines and provider info, and calculates mathematical deviations.
4. **`alerter.py`**: The Business Logic core. Evaluates the enriched data against predefined thresholds (`MIN_FAILURE`, `MIN_VOLUME`) and categorizes the data into actionable subsets.
5. **`reporter.py`**: The Presentation layer. Formats columns, rounds values, and uses `openpyxl` to generate a multi-sheet, color-coded Excel report.
6. **`main.py`**: The Orchestrator. Parses arguments and runs the entire pipeline end-to-end with a single command.

---

## 📦 Features

### ✅ Currently Implemented
* **Automated End-to-End Pipeline**: Run extraction, transformation, and reporting with a single command.
* **Execution Modes**:
  * **Standard Mode**: Extracts data for a specific time window (e.g., `now-7d`) and generates a current alert report.
  * **Historic Mode**: Incrementally updates the historical baseline without reprocessing the last 10 years of data.
* **Alert Types Evaluated**:
  * **Technical Failure Rate**: Evaluates failed operations against custom thresholds depending on technical configurations (e.g., `politics_search`).
  * **Volume Deviation**: Detects steep drops in daily operational volume compared to the historical baseline.
* **Severity Levels**: Categorizes alerts into `NORMAL` (Green), `CONCERN` (Yellow), `SEVERE` (Red), and `URGENT` (Dark Red).
* **Decoupled Configuration**: Fully configurable via `.env` files.

### 🔮 Roadmap (Coming Soon)
* **Broader Entity Grouping**: While the system architecture supports grouping by `hotel_name`, `client_name`, or `destination_name`, the default orchestration is currently optimized for `provider_name`. Full automated support for other dimensions is in progress.
* **Automated Notifications**: Direct integration with Slack API and Email SMTP to send alerts automatically, bypassing the need to check the Excel file manually.
* **Advanced Anomaly Detection**: Moving from static thresholds (`-25%`, `10%`) to moving-average baselines and statistical Machine Learning models.
* **Cron Scheduling**: Fully automated daily executions.

---

## 📂 Project Structure

```text
data-alerts/
│
├── alerter.py                 # Business logic and threshold evaluation
├── config.py                  # Global configurations and env loading
├── extractor.py               # Kibana data extraction
├── reporter.py                # Excel generation and styling
├── transformer.py             # Data aggregation and historical merge
├── main.py                    # Pipeline orchestrator
│
├── .env                       # Environment variables (DO NOT COMMIT)
├── .env.example               # Template for environment variables
├── .pylintrc                  # Linter configuration rules
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

---

## ⚙️ Setup & Configuration

1. **Clone the repository** to your local machine.

2. **Set up a Virtual Environment (Recommended)**:
   It is highly recommended to run this project inside a virtual environment to isolate its dependencies.
   ```bash
   # Create the virtual environment
   python -m venv venv

   # Activate it on Windows:
   venv\Scripts\activate

   # Activate it on macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Variables**: Create a `.env` file in the root directory based on the provided template.

### `.env.example`
```env
# =========================
# Data Inputs & Outputs
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
# Alert File Mappings
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
# Kibana Configuration
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
# Alert Thresholds
# =========================
# Minimum failure percentage to trigger an alert
MIN_FAILURE=10
# Minimum volume drop percentage to trigger an alert (negative number)
MIN_VOLUME=-25
```

---

## ▶️ Usage

Thanks to the new orchestration module, you no longer need to run scripts individually. Use `main.py` to drive the entire flow.

### 1. Standard Run (Default)
Extracts the last 7 days of data, groups by provider, evaluates alerts, and generates the Excel report.
```bash
python main.py
```

### 2. Custom Time Range
Specify a custom ElasticSearch time expression (e.g., `now-14d`, `now-24h`).
```bash
python main.py now-14d
```

### 3. Custom Grouping (Experimental)
Specify both the time range and the entity you want to group by.
```bash
python main.py now-10d hotel_name
```

### 4. Update Historical Baseline
Downloads only the new records since the last extraction and updates your historical accumulated data. *Note: This mode skips the Excel report generation.*
```bash
python main.py historic
```