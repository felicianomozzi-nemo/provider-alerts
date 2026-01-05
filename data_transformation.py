"""
Provider Operations Consolidation Script

This script loads operational data from multiple CSV files,
aggregates booking flow information by provider and exports a consolidated CSV
file that serves as input for downstream alerting processes.

The script is period-agnostic: it works with any time range
contained in the input CSVs.
"""

import os
import pandas as pd
from dotenv import load_dotenv


# =========================
# Configuration
# =========================

load_dotenv()

BOOKING_FLOW_CSV = os.getenv("BOOKING_FLOW_URL")
SUMMARY_OUTPUT_CSV = os.getenv("SUMMARY_OUTPUT_CSV_URL")


# =========================
# Data loading
# =========================

def load_csvs() -> tuple[pd.DataFrame]:
    """
    Load required CSV files into pandas DataFrames.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - Booking Flow data
    """
    booking_flow = pd.read_csv(BOOKING_FLOW_CSV)
    return booking_flow

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
    booking_flow = load_csvs()

    provider_summary = build_provider_summary(booking_flow)

    export_csv(provider_summary)

    print(f"CSV consolidado generado: {SUMMARY_OUTPUT_CSV}")


if __name__ == "__main__":
    main()
