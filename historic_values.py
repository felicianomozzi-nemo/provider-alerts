"""
Provider Historic Values Script

This script generates historic provider metrics using the same
aggregation logic as the operational data transformation,
but applied to a long-term booking_flow dataset (e.g. last 2 years).
"""

import os
import pandas as pd
from dotenv import load_dotenv


# =========================
# Configuration
# =========================

load_dotenv()

BOOKING_FLOW_HISTORIC_CSV = os.getenv("HISTORIC_BOOKING_FLOW_URL")
HISTORIC_OUTPUT_CSV = os.getenv("HISTORIC_OUTPUT_CSV_URL")


# =========================
# Data loading
# =========================

def load_csv() -> pd.DataFrame:
    """
    Load historical booking flow CSV.
    """
    data = pd.read_csv(BOOKING_FLOW_HISTORIC_CSV)
    return data

# =========================
# Data transformation
# =========================

def build_provider_summary(booking_flow_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate booking flow data by provider and compute
    operational metrics.
    """
    df = booking_flow_df.copy()

    df["success"] = df["success"].astype(bool)

    summary = (
        df.groupby("providername")
        .agg(
            total_operations=("success", "count"),
            successful_operations=("success", "sum"),
            failed_operations=("success", lambda x: (~x).sum())
        )
        .reset_index()
    )

    summary["failure_rate"] = (
        summary["failed_operations"] / summary["total_operations"]
    )

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
    Export historic provider metrics to CSV.
    """
    dataframe.to_csv(HISTORIC_OUTPUT_CSV, index=False)


# =========================
# Main
# =========================

def main() -> None:
    """
    Main execution flow.
    """
    booking_flow = load_csv()

    provider_summary = build_provider_summary(booking_flow)

    export_csv(provider_summary)

    print(f"CSV histórico generado: {HISTORIC_OUTPUT_CSV}")

if __name__ == "__main__":
    main()
