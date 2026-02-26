"""
This script loads operational data from multiple CSV files,
aggregates booking flow information by a designated column and exports a consolidated CSV
file that serves as input for downstream alerting processes.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

# =========================
# Configuration
# =========================
load_dotenv()

CURRENT_DATA = os.getenv("CURRENT_DATA")
HISTORIC_DATA = os.getenv("HISTORIC_DATA")

SUMMARY_DIR = os.getenv("SUMMARY_DIR")
HISTORIC_SUMMARY_DIR = os.getenv("HISTORIC_SUMMARY_DIR")
SUMMARY_ENDING = os.getenv("SUMMARY_ENDING")

# =========================
# Data transformation
# =========================
def calculate_period_days(df: pd.DataFrame) -> int:
    """
    Calculate number of real days covered by dataset, based on @timestamp column.
    """

    if "@timestamp" not in df.columns:
        raise ValueError("@timestamp column is required to calculate period")
    df["@timestamp"] = pd.to_datetime(df["@timestamp"])

    min_date = df["@timestamp"].min()
    max_date = df["@timestamp"].max()

    period_days = (max_date - min_date).days + 1

    return max(period_days, 1)


def build_summary(booking_flow_df: pd.DataFrame, group_by_column: str) -> pd.DataFrame:
    """
    Aggregate booking flow data by a designated column and compute
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
            Aggregated column-level summary.
    """
    # Defensive copy
    df = booking_flow_df.copy()

    period_days = calculate_period_days(df)

    # Normalize success column to boolean
    df["success"] = df["success"].astype(bool)

    # Aggregate metrics by designated column
    summary = (
        df.groupby(group_by_column)
        .agg(
            total_operations=("success", "count"),
            successful_operations=("success", "sum"),
            failed_operations=("success", lambda x: (~x).sum())
        )
        .reset_index()
    )

    summary["period_days"] = period_days

    summary["daily_avg_operations"] = (
        summary["total_operations"] / period_days
    )

    # Derived metrics
    summary["failure_rate"] = (
        summary["failed_operations"] / summary["total_operations"]
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
        2. Calculates metrics and generates a Summary
        3. Exports it to a CSV file
    """
    try:
        # Parse arguments
        if len(sys.argv) > 1:
            group_by_column = sys.argv[1]
        else:
            group_by_column = "provider_name" # default
        mode = "standard"

        if len(sys.argv) > 2 and sys.argv[2] == "historic":
            mode = "historic"
        # Select input/output paths
        if mode == "historic":
            input_path = HISTORIC_DATA
            output_path = HISTORIC_SUMMARY_DIR + group_by_column + SUMMARY_ENDING
            print(f"Running HISTORIC transformation mode from {input_path} to {output_path}")
        else:
            time_range = sys.argv[2]
            input_path = CURRENT_DATA + time_range + "_data.csv"
            output_path = SUMMARY_DIR + "historic_" + group_by_column + "_" + time_range + SUMMARY_ENDING
            print(f"Running STANDARD transformation mode from {input_path} to {output_path}")
        # Load CSV
        booking_flow_df = pd.read_csv(input_path)

        if booking_flow_df.empty:
            print("Input CSV is empty. Nothing to process.")
            return

        # Build summary
        summary_df = build_summary(booking_flow_df, group_by_column)
        print(f"Exporting summary to: {output_path}")
        export_csv(summary_df, output_path)

        print("Transformation completed successfully")
    except Exception as e:
        print(f"Error during transformation: {e}")

if __name__ == "__main__":
    main()
