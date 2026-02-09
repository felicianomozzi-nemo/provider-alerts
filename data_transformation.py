"""
Provider Operations Consolidation Script

This script loads operational data from multiple CSV files,
aggregates booking flow information by provider and exports a consolidated CSV
file that serves as input for downstream alerting processes.

The script is period-agnostic: it works with any time range
contained in the input CSVs.
"""

import os
import socket
import unicodedata
from urllib.parse import urlparse
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth



# =========================
# Configuration
# =========================

load_dotenv()

BOOKING_FLOW_URL = os.getenv("BOOKING_FLOW_URL")
SUMMARY_OUTPUT_CSV = os.getenv("SUMMARY_OUTPUT_CSV_URL")

# --- Kibana APM (Infraestructure - Nemo) ---
KIBANA_APM_URL = os.getenv("KIBANA_APM_URL")
KIBANA_APM_USER = os.getenv("KIBANA_APM_USER")
KIBANA_APM_PASS = os.getenv("KIBANA_APM_PASS")
KIBANA_APM_AUTH = HTTPBasicAuth(KIBANA_APM_USER, KIBANA_APM_PASS)
KIBANA_APM_HEADERS = os.getenv("KIBANA_APM_HEADERS")

# --- Kibana BUSINESS (Business Events - PSurfer) ---
KIBANA_BUSINESS_URL = os.getenv("KIBANA_BUSINESS_URL")
KIBANA_BUSINESS_USER = os.getenv("KIBANA_BUSINESS_USER")
KIBANA_BUSINESS_PASS = os.getenv("KIBANA_BUSINESS_PASS")
KIBANA_BUSINESS_AUTH = HTTPBasicAuth(KIBANA_BUSINESS_USER, KIBANA_BUSINESS_PASS)
KIBANA_BUSINESS_HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

# ============================================================================
# Connection functions
# ============================================================================

def verify_kibana_connection(url, username, password, headers=None, name="Kibana"):
    """
    Verifies the connection to a Kibana server and the authentication
    
    Args:
        url: URL of the Kibana server
        username: User for the authentication
        password: Password for the authentication
        headers: Dictionary of personalized headers (optional)
        name: Name of the server (designated for logs)
    
    Returns:
        bool: True if the connection and the authentication are succesful
    """
    # Default headers if they're not set
    if headers is None:
        headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
    auth = HTTPBasicAuth(username, password)
    # Extracts host and port from the URL
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
    print("=" * 70)
    print(f"CONNECTION VERIFICATION: {name}")
    print(f"URL: {url}")
    print("=" * 70)
    # 1. Verifies connectivity with socket
    print(f"\n1. Verifying connectivity to {host}:{port}...")
    try:
        socket.create_connection((host, port), timeout=5)
        print(f"   ✓ Server reachable at {host}:{port}")
        connectivity_ok = True
    except (socket.timeout, socket.error) as e:
        print(f"   ✗ Can't reach server: {e}")
        connectivity_ok = False
        print("\n" + "=" * 70)
        print(f"✗ CONNECTION {name.upper()}: FAILED (Not reachable)")
        print("=" * 70)
        return False
    # 2. Verifies authentication
    print(f"\n2. Verifying authentication at {name}...")
    try:
        # Tries hiting and endpoint that needs authentication
        response = requests.get(
            f"{url}/api/status",
            auth=auth,
            headers=headers,
            timeout=10,
            verify=False
        )
        if response.status_code == 200:
            print(f"   ✓ Authentication succesful (Status: {response.status_code})")
            print(f"   ✓ {name} is available and accesible")
            auth_ok = True
        elif response.status_code == 401:
            print(f"   ✗ Wrong or invalid credentials (Status: {response.status_code})")
            print(f"      User: {username}")
            auth_ok = False
        elif response.status_code == 403:
            print(f"   ✗ Access denied {response.status_code}")
            auth_ok = False
        else:
            print(f"   ! Unexpected Status: {response.status_code}")
            print(f"      Response: {response.text[:200]}")
            auth_ok = response.status_code < 400
    except requests.exceptions.ConnectionError as e:
        print(f"   ✗ Connection error: {e}")
        auth_ok = False
    except requests.exceptions.Timeout as e:
        print(f"   ✗ Connection timeout: {e}")
        auth_ok = False
    except Exception as e:
        print(f"   ✗ Unexpected error: {e}")
        auth_ok = False
    # 3. Resume
    print("\n" + "=" * 70)
    if connectivity_ok and auth_ok:
        print(f"✓ CONNECTION {name.upper()}: OK")
    else:
        print(f"✗ CONNECTION {name.upper()}: FAILED")
    print("=" * 70)
    return connectivity_ok and auth_ok

# --- Verifies both connections  ---
result_apm = verify_kibana_connection(
    url=KIBANA_APM_URL,
    username=KIBANA_APM_USER,
    password=KIBANA_APM_PASS,
    headers=KIBANA_APM_HEADERS,
    name="Kibana APM"
)

result_business = verify_kibana_connection(
    url=KIBANA_BUSINESS_URL,
    username=KIBANA_BUSINESS_USER,
    password=KIBANA_BUSINESS_PASS,
    headers=KIBANA_BUSINESS_HEADERS,
    name="Kibana BUSINESS"
)

# =========================
# Data loading
# =========================

def normalize_name(name):
    """Normalices names: removes weird characters and standarizes spaces"""
    if pd.isna(name):
        return name
    name_normalized = ''.join(c for c in unicodedata.normalize('NFD', str(name)) if unicodedata.category(c) != 'Mn')

    return name_normalized.strip().title()

def load_csvs(hours=1) -> tuple[pd.DataFrame]:
    """
    Load required CSV files into pandas DataFrames.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - Booking Flow data
    """
    endpoint = f"{KIBANA_BUSINESS_URL}/api/console/proxy"
    params = {"path": f"{BOOKING_FLOW_URL}/_search", "method": "POST"}
    query = {
        "size": 10000,
        "_source": ["@timestamp", "clientname", "operation", "providername", "destinationname", "checkin", "checkout", "success", "hotelname"],
        "query": {
            "range": {
                "@timestamp": { "gte": f"now-{hours}h"}
            }
        }
    }

    try:
        response = requests.post(endpoint, params=params, auth=KIBANA_BUSINESS_AUTH, headers=KIBANA_BUSINESS_HEADERS, json=query, verify=False)
        res_json = response.json()

        if 'hits' not in res_json:
            print("Error: Data not found")
            print(f"Response: {res_json}")
            return pd.DataFrame()
        hits = res_json['hits']['hits']
        if len(hits) == 0:
            print("There are no registers in the time specified")
            return pd.DataFrame()
        df = pd.DataFrame([h['_source'] for h in hits])

        df['destinationname'] = df['destinationname'].apply(normalize_name)
        df['clientname'] = df['clientname'].apply(normalize_name)
        df['providername'] = df['providername'].apply(normalize_name)
        df['hotelname'] = df['hotelname'].apply(normalize_name)

        return df
    except Exception as e:
        print(f"Error in the extraction: {e}")
    return pd.DataFrame()

# =========================
# Data transformation
# =========================

def build_provider_summary(booking_flow_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate booking flow data by provider and compute
    basic operational metrics.

    Metrics calculated:
        - Total operations
        - Successful operations
        - Failed operations
        - Failure rate

    Args:
        booking_flow_df (pd.DataFrame):
            Raw booking flow data.

    Returns:
        pd.DataFrame:
            Aggregated provider-level summary.
    """
    # Defensive copy
    df = booking_flow_df.copy()

    # Normalize success column to boolean
    df["success"] = df["success"].astype(bool)

    # Aggregate metrics by provider
    summary = (
        df.groupby("providername")
        .agg(
            total_operations=("success", "count"),
            successful_operations=("success", "sum"),
            failed_operations=("success", lambda x: (~x).sum())
        )
        .reset_index()
    )

    # Derived metrics
    summary["failure_rate"] = (
        summary["failed_operations"] / summary["total_operations"]
    )

    # Standardize column naming
    summary.rename(
        columns={"providername": "provider_name"},
        inplace=True
    )

    return summary

# =========================
# Export
# =========================

def export_csv(dataframe: pd.DataFrame) -> None:
    """
    Export the consolidated provider summary to a CSV file.

    Args:
        dataframe (pd.DataFrame):
            Final provider summary to be exported.
    """
    dataframe.to_csv(SUMMARY_OUTPUT_CSV, index=False)


# =========================
# Main application
# =========================

def main() -> None:
    """
    Main execution flow of the script.

    Steps:
        1. Load input CSV files
        2. Build provider-level operational summary
        3. Export consolidated CSV
    """
    raw_data = load_csvs()
    refined_data = build_provider_summary(raw_data)
    export_csv(refined_data)

if __name__ == "__main__":
    main()
