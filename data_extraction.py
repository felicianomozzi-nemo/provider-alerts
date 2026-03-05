"""
This scripts connects directly to Kibana and extracts the data needed.
"""

import os
import sys
import socket
import unicodedata
import traceback
from urllib.parse import urlparse
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# =========================
# Configuration
# =========================

load_dotenv()

# DIR info
HISTORIC_DATA = os.getenv("HISTORIC_DATA")
CURRENT_DATA = os.getenv("CURRENT_DATA")

# Index info
INDEX_URL = os.getenv("INDEX_URL")

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

def normalize_name(name: str) -> str:
    """Normalices names: removes weird characters and standarizes spaces
    
    Args:
        name (str): name of the column to be standarized
    
    Returns:
        name_normalized:
            - The name but now it is normalized
    """
    if pd.isna(name):
        return name
    name_normalized = ''.join(c for c in unicodedata.normalize('NFD', str(name)) if unicodedata.category(c) != 'Mn')

    return name_normalized.strip().title()

def load_csvs(time_range: str) -> tuple[pd.DataFrame]:
    """
    Load required CSV files into pandas DataFrames.

    Args:
        time_range (str): Elasticsearch time expression (e.g. "now-10h", "now-30d", "now-1y")
    
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - Booking Flow data
    """
    # Validates if it should run from a specific timestamp or just go with the elastic time expression
    operator = "gte"
    if "T" in time_range and ":" in time_range:
        operator = "gt"

    try:
        endpoint = f"{KIBANA_BUSINESS_URL}/api/console/proxy"
        params = {"path": f"{INDEX_URL}/_search", "method": "POST"}

        print(f"\n📡 Consulting ALL business records TODOS los registros de Negocio (time period: {time_range})...")
        print("Utilizing Search After (without result_window limit)...")

        # First query to initiate the search_after
        initial_query = {
            "size": 5000,
            "_source": ["@timestamp", "clientname", "operation", "providername", "destinationname", "checkin", "checkout", "success", "hotelname"],
            "sort": [{"@timestamp": "asc"}],
            "query": {
                "range": {
                    "@timestamp": { operator: time_range}
                }
            }
        }

        all_hits = []
        page_size = 5000
        batch = 1
        search_after = None
        total_downloaded_bytes = 0

        while True:
            print(f"\n Batch {batch}: Extracting {page_size} registers...")

            # Construct query with search_after if its not the first batch
            query = initial_query.copy()
            if search_after is not None:
                query["search_after"] = search_after

            try:
                response = requests.post(endpoint, params=params, auth=KIBANA_BUSINESS_AUTH, headers=KIBANA_BUSINESS_HEADERS, json=query, verify=False)

                # Calculate the response size
                bytes_batch = len(response.content)
                total_downloaded_bytes = total_downloaded_bytes + bytes_batch

                if response.status_code != 200:
                    print(f"Status code in batch {batch}: {response.status_code}")
                    print(f"Response: {response.text[:300]}")
                    break

                res_json = response.json()
                hits = res_json.get('hits', {}).get('hits', [])

                if not hits:
                    print("There are no more registers. End of the extraction")
                    break

                all_hits.extend(hits)
                obtained_registers = len(all_hits)

                # Formats the batch size
                if bytes_batch < 1024:
                    size_str = f"{bytes_batch} B"
                elif bytes_batch < 1024 * 1024:
                    size_str = f"{bytes_batch / 1024:.1f} KB"
                else:
                    size_str = f"{bytes_batch / (1024*1024):.2f} MB"

                print(f"+{len(hits)} registers ({size_str}). Total accumulated: {obtained_registers}")

                # If we have less registers than the page_size it means we reached the end
                if len(hits) < page_size:
                    print(f" Last page ({len(hits)} registers < {page_size}). End of the extraction.")
                    break

                # Prepares search_after for the next batch using the last document
                last_hit = hits[-1]
                search_after = last_hit.get('sort', [])

                batch = batch + 1

            except Exception as e:
                print(f" Error in batch {batch}: {e}")
                break
        # Show the total downloaded size
        if total_downloaded_bytes < 1024:
            total_size_str = f"{total_downloaded_bytes} bytes"
        elif total_downloaded_bytes < 1024*1024:
            total_size_str = f"{total_downloaded_bytes / 1024:.2f} KB"
        else:
            total_size_str = f"{total_downloaded_bytes / 1024*1024:.2f} MB"
        print(f"\n Extraction completed: {len(all_hits)} registers")
        print(f"Total downloaded: {total_size_str}")

        if len(all_hits) == 0:
            print("There aren't any registers in the time range specified")
            return pd.DataFrame()
        # Process data
        df = pd.DataFrame([h['_source'] for h in all_hits])

        # Calculate size in memory of the data frame
        df_memory = df.memory_usage(deep=True).sum()
        if df_memory < 1024:
            df_size_str = f"{df_memory} bytes"
        elif df_memory < 1024 * 1024:
            df_size_str = f"{df_memory / 1024:.2f} KB"
        else:
            df_size_str = f"{df_memory / (1024 * 1024):.2f} MB"
        print(f"Size in memory (DataFrame): {df_size_str}")

        # Normalize names
        df['destinationname'] = df['destinationname'].apply(normalize_name)
        df['clientname'] = df['clientname'].apply(normalize_name)
        df['providername'] = df['providername'].apply(normalize_name)
        df['hotelname'] = df['hotelname'].apply(normalize_name)
        df = df.rename(columns={
            "providername": "provider_name",
            "clientname": "client_name",
            "destinationname": "destination_name",
            "hotelname": "hotel_name"
        })

        return df
    except Exception as e:
        print(f"Error in the extraction: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def load_full_history() -> pd.DataFrame:
    """
    Loads and returns a dataframe with the data from the last 10 years

    Returns:
        dataframe (pd.DataFrame):
            Data from the last 10 years
    """
    return load_csvs("now-10y")

# =========================
# Export
# =========================

def export_csv(dataframe: pd.DataFrame, output_url: str) -> None:
    """
    Export the dataframe obtained to a CSV file.

    Args:
        dataframe (pd.DataFrame):
            Data to be exported
    """
    dataframe.to_csv(output_url, index=False)

# =========================
# Modes
# =========================

def run_standard_mode(time_range: str):
    """
    Runs the script in the 'Standard Mode' which means that it has a specific time range
    
    Args:
        time_range:
            Time Range of the period of days of interest
    """
    print(f"Extracting data for time range: {time_range}")
    df = load_csvs(time_range=time_range)
    output_url = CURRENT_DATA + time_range + "_data.csv"
    export_csv(df, output_url)

def run_historic_mode():
    """
    Runs the script in the historic mode, which means it'll download ALL data available if there is no
    historic file already. If it finds a current historic file, it'll read its last timestamp and request
    data from that timestamp to today.
    """
    print("Running HISTORIC incremental mode")

    if not os.path.exists(HISTORIC_DATA):
        print("Historic file not found. Downloading full history...")
        df = load_full_history()
        export_csv(df, HISTORIC_DATA)
        print("Full historic download completed")
        return
    print("Historic file found. Loading existing data...")
    existing_df = pd.read_csv(HISTORIC_DATA)

    if existing_df.empty:
        print("Historic file empty. Downloading full history...")
        df = load_full_history()
        export_csv(df, HISTORIC_DATA)
        return
    last_timestamp = pd.to_datetime(existing_df["@timestamp"].max()).isoformat()
    print(f"Last timestamp found: {last_timestamp}")

    new_data = load_csvs(last_timestamp)

    if new_data.empty:
        print("No new data found")
        return
    updated_df = pd.concat([existing_df, new_data], ignore_index=True)
    export_csv(updated_df, HISTORIC_DATA)

    print("Historic file updated successfully")

# =========================
# Main application
# =========================

def main() -> None:
    """
    Main execution flow of the script.

    Steps:
    1- Understands the mode requested
    2- Connects to Kibana to donwload data
    3- Runs the mode that it identified (standard or historic)
    4- Downloads the data requested
    5- Exports it to a CSV in the route specified in the .ENV files
    """
    if len(sys.argv) > 1:
        param = sys.argv[1]
    else:
        param = "now-10h" # default
    if param == "historic":
        run_historic_mode()
    else:
        run_standard_mode(param)

if __name__ == "__main__":
    main()
