"""
Data Extraction Module.
Handles Kibana connections, data retrieval, and initial normalization.
"""

import os
import socket
import unicodedata
import traceback
from urllib.parse import urlparse
import pandas as pd
import requests

import config

def verify_kibana_connection(url: str, username: str, password: str, headers: dict = None, name: str = "Kibana") -> bool:
    """
    Verifies the connection to a Kibana server and the authentication.
    
    Args:
        url: URL of the Kibana server.
        username: User for the authentication.
        password: Password for the authentication.
        headers: Dictionary of personalized headers (optional).
        name: Name of the server (designated for logs).
    
    Returns:
        bool: True if the connection and the authentication are successful.
    """
    if headers is None:
        headers = {"kbn-xsrf": "true", "Content-Type": "application/json"}
    auth = requests.auth.HTTPBasicAuth(username, password)
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
    print("=" * 70)
    print(f"CONNECTION VERIFICATION: {name}")
    print(f"URL: {url}")
    print("=" * 70)
    print(f"\n1. Verifying connectivity to {host}:{port}...")
    try:
        socket.create_connection((host, port), timeout=5)
        print(f"   ✓ Server reachable at {host}:{port}")
        connectivity_ok = True
    except (socket.timeout, socket.error) as error:
        print(f"   ✗ Can't reach server: {error}")
        connectivity_ok = False
        print("\n" + "=" * 70)
        print(f"✗ CONNECTION {name.upper()}: FAILED (Not reachable)")
        print("=" * 70)
        return False
    print(f"\n2. Verifying authentication at {name}...")
    try:
        response = requests.get(
            f"{url}/api/status",
            auth=auth,
            headers=headers,
            timeout=10,
            verify=False
        )
        if response.status_code == 200:
            print(f"   ✓ Authentication successful (Status: {response.status_code})")
            auth_ok = True
        elif response.status_code == 401:
            print(f"   ✗ Wrong or invalid credentials (Status: {response.status_code})")
            auth_ok = False
        elif response.status_code == 403:
            print(f"   ✗ Access denied {response.status_code}")
            auth_ok = False
        else:
            print(f"   ! Unexpected Status: {response.status_code}")
            auth_ok = response.status_code < 400
    except requests.exceptions.RequestException as error:
        print(f"   ✗ Connection error: {error}")
        auth_ok = False
    print("\n" + "=" * 70)
    if connectivity_ok and auth_ok:
        print(f"✓ CONNECTION {name.upper()}: OK")
    else:
        print(f"✗ CONNECTION {name.upper()}: FAILED")
    print("=" * 70)
    return connectivity_ok and auth_ok

def normalize_name(name: str) -> str:
    """
    Normalizes names: removes weird characters and standardizes spaces.
    
    Args:
        name (str): Name of the column to be standardized.
    
    Returns:
        str: The normalized name.
    """
    if pd.isna(name):
        return name
    name_normalized = ''.join(
        c for c in unicodedata.normalize('NFD', str(name))
        if unicodedata.category(c) != 'Mn'
    )
    return name_normalized.strip().title()

def load_csvs(time_range: str) -> pd.DataFrame:
    """
    Load required business records from Kibana into a pandas DataFrame.

    Args:
        time_range (str): Elasticsearch time expression (e.g., "now-10h").
    
    Returns:
        pd.DataFrame: DataFrame containing booking flow data.
    """
    operator = "gt" if "T" in time_range and ":" in time_range else "gte"

    try:
        endpoint = f"{config.KIBANA_BUSINESS_URL}/api/console/proxy"
        params = {"path": f"{config.INDEX_URL}/_search", "method": "POST"}

        print(f"\n📡 Consulting ALL business records (time period: {time_range})...")
        initial_query = {
            "size": 5000,
            "_source": [
                "@timestamp", "clientname", "operation", "providername", 
                "destinationname", "checkin", "checkout", "success", "hotelname"
            ],
            "sort": [{"@timestamp": "asc"}],
            "query": {"range": {"@timestamp": {operator: time_range}}}
        }

        all_hits = []
        page_size = 5000
        batch = 1
        search_after = None
        total_downloaded_bytes = 0

        while True:
            print(f"\n Batch {batch}: Extracting {page_size} registers...")
            query = initial_query.copy()
            if search_after is not None:
                query["search_after"] = search_after

            try:
                response = requests.post(
                    endpoint,
                    params=params,
                    auth=config.KIBANA_BUSINESS_AUTH,
                    headers=config.KIBANA_BUSINESS_HEADERS,
                    json=query,
                    verify=False
                )
                bytes_batch = len(response.content)
                total_downloaded_bytes += bytes_batch

                if response.status_code != 200:
                    print(f"Status code in batch {batch}: {response.status_code}")
                    break

                res_json = response.json()
                hits = res_json.get('hits', {}).get('hits', [])

                if not hits:
                    print("There are no more registers. End of the extraction.")
                    break

                all_hits.extend(hits)
                print(f"+{len(hits)} registers. Total accumulated: {len(all_hits)}")

                if len(hits) < page_size:
                    print(f" Last page ({len(hits)} registers < {page_size}). End of the extraction.")
                    break

                search_after = hits[-1].get('sort', [])
                batch += 1

            except requests.exceptions.RequestException as error:
                print(f" Error in batch {batch}: {error}")
                break

        print(f"\n Extraction completed: {len(all_hits)} registers extracted.")

        if not all_hits:
            return pd.DataFrame()

        df = pd.DataFrame([h['_source'] for h in all_hits])

        # Provider info normalization
        if os.path.exists(config.PROVIDER_INFO_URL):
            provider_info = pd.read_csv(config.PROVIDER_INFO_URL)
            provider_info["provider_name"] = provider_info["provider_name"].apply(normalize_name)
            provider_info.to_csv(config.PROVIDER_INFO_URL, index=False)

        # Normalize specific columns
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

    except Exception as error:
        print(f"Error in the extraction: {error}")
        traceback.print_exc()
        return pd.DataFrame()

def load_full_history() -> pd.DataFrame:
    """Loads data from the last 10 years."""
    return load_csvs("now-10y")

def run_extraction(time_range: str, mode: str = "standard") -> None:
    """
    Executes the data extraction pipeline based on the requested mode.
    
    Args:
        time_range (str): Time range to extract.
        mode (str): Execution mode ('standard' or 'historic').
    """
    if mode == "historic":
        print("Running HISTORIC incremental mode...")
        if not os.path.exists(config.HISTORIC_DATA):
            df = load_full_history()
            df.to_csv(config.HISTORIC_DATA, index=False)
            return
        existing_df = pd.read_csv(config.HISTORIC_DATA)
        if existing_df.empty:
            df = load_full_history()
            df.to_csv(config.HISTORIC_DATA, index=False)
            return
        last_timestamp = pd.to_datetime(existing_df["@timestamp"].max()).isoformat()
        new_data = load_csvs(last_timestamp)
        if not new_data.empty:
            updated_df = pd.concat([existing_df, new_data], ignore_index=True)
            updated_df.to_csv(config.HISTORIC_DATA, index=False)
            print("Historic file updated successfully.")
    else:
        print(f"Extracting standard data for time range: {time_range}")
        df = load_csvs(time_range=time_range)
        if not df.empty:
            output_url = f"{config.CURRENT_DATA}{time_range}_data.csv"
            df.to_csv(output_url, index=False)
            print(f"Standard extraction saved to {output_url}")
        else:
            print("No data extracted for the given time range.")
