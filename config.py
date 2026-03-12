"""
Configuration module.
Loads environment variables and defines global constants used across the application.
"""

import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# Load environment variables from .env file
load_dotenv()

# =========================
# Directories and Files
# =========================
HISTORIC_DATA = os.getenv("HISTORIC_DATA")
CURRENT_DATA = os.getenv("CURRENT_DATA")
PROVIDER_INFO_URL = os.getenv("PROVIDER_INFO_URL")

SUMMARY_DIR = os.getenv("SUMMARY_DIR")
SUMMARY_ENDING = os.getenv("SUMMARY_ENDING")
HISTORIC_SUMMARY_DIR = os.getenv("HISTORIC_SUMMARY_DIR")

OUTPUT_DIR = os.getenv("OUTPUT_DIR")
OUTPUT_ENDING = os.getenv("OUTPUT_ENDING")

# Input files mapping for alerts
INPUT_FILES = {
    "provider_name": os.getenv("PROVIDERS_SUMMARY_CSV"),
    "hotel_name": os.getenv("HOTELS_SUMMARY_CSV"),
    "client_name": os.getenv("CLIENTS_SUMMARY_CSV"),
    "destination_name": os.getenv("DESTINATIONS_SUMMARY_CSV"),
}

HISTORIC_FILES = {
    "provider_name": os.getenv("HISTORIC_PROVIDERS_SUMMARY_CSV"),
    "hotel_name": os.getenv("HISTORIC_HOTELS_SUMMARY_CSV"),
    "client_name": os.getenv("HISTORIC_CLIENTS_SUMMARY_CSV"),
    "destination_name": os.getenv("HISTORIC_DESTINATIONS_SUMMARY_CSV"),
}

# =========================
# Kibana Configuration
# =========================
INDEX_URL = os.getenv("INDEX_URL")

# APM
KIBANA_APM_URL = os.getenv("KIBANA_APM_URL")
KIBANA_APM_USER = os.getenv("KIBANA_APM_USER")
KIBANA_APM_PASS = os.getenv("KIBANA_APM_PASS")
KIBANA_APM_AUTH = HTTPBasicAuth(KIBANA_APM_USER, KIBANA_APM_PASS) if KIBANA_APM_USER else None
KIBANA_APM_HEADERS = os.getenv("KIBANA_APM_HEADERS")

# Business
KIBANA_BUSINESS_URL = os.getenv("KIBANA_BUSINESS_URL")
KIBANA_BUSINESS_USER = os.getenv("KIBANA_BUSINESS_USER")
KIBANA_BUSINESS_PASS = os.getenv("KIBANA_BUSINESS_PASS")
KIBANA_BUSINESS_AUTH = HTTPBasicAuth(KIBANA_BUSINESS_USER, KIBANA_BUSINESS_PASS) if KIBANA_BUSINESS_USER else None
KIBANA_BUSINESS_HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

# =========================
# Thresholds
# =========================
MIN_FAILURE = float(os.getenv("MIN_FAILURE", "10"))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "-25"))
