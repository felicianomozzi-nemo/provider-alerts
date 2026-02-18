"""
This script loads operational data from multiple CSV files,
aggregates booking flow information by provider and exports a consolidated CSV
file that serves as input for downstream alerting processes.
"""

import os
import pandas as pd
from dotenv import load_dotenv

# =========================
# Configuration
# =========================
load_dotenv()
DATA_INPUT_URL = os.getenv("RAW_OUTPUT_CSV_URL")
PROVIDERS_SUMMARY_OUTPUT_URL = os.getenv("SUMMARY_OUTPUT_CSV_URL")
HISTORIC_PROVIDERS_SUMMARY_OUTPUT_URL = os.getenv("HISTORIC_OUTPUT_CSV_URL")

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

def export_csv(dataframe: pd.DataFrame, output_url: str) -> None:
    """
    Export the dataframe obtained to a CSV file.

    Args:
        dataframe (pd.DataFrame):
            Data to be exported
    """
    dataframe.to_csv(output_url, index=False)

# =========================
# Main application
# =========================

def main() -> None:
    """
    Main execution flow of the script.

    Steps:
        1. Loads CSV file from the directory provided
        2. Calculates metrics and generates Providers Summary
        3. Exports it to a CSV file
    """
    try:
        print(f"Reading input file: {DATA_INPUT_URL}")

        booking_flow_df = pd.read_csv(DATA_INPUT_URL)

        if booking_flow_df.empty:
            print("Input CSV is empty. Nothing to process.")
            return
        summary_df = build_provider_summary(booking_flow_df)

        print(f"Exporting provider summary to: {PROVIDERS_SUMMARY_OUTPUT_URL}")
        export_csv(summary_df, PROVIDERS_SUMMARY_OUTPUT_URL)

        print("Transformation completed successfully")
    except Exception as e:
        print(f"Error during transformation: {e}")

if __name__ == "__main__":
    main()
