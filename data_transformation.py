"""
This script loads operational data from multiple CSV files,
aggregates booking flow information by provider and exports a consolidated CSV
file that serves as input for downstream alerting processes.
"""

import pandas as pd

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

if __name__ == "__main__":
    main()
