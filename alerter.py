"""
Alert Evaluation Module.
Reads the consolidated and enriched summary, applies business logic and thresholds,
and categorizes data into different alert subsets.
"""

import os
import pandas as pd
import numpy as np
import config

def classify_failure_alert(rate: float, flag: bool) -> str:
    """Classifies failure rate alerts based on technical configuration."""
    if pd.isna(rate):
        return "CAN'T EVALUATE"
    if not flag:
        if rate >= (config.MIN_FAILURE * 3):
            return "URGENT"
        if rate >= (config.MIN_FAILURE * 2):
            return "SEVERE"
        if rate >= (config.MIN_FAILURE * 1.5):
            return "CONCERN"
    else:
        if rate >= (config.MIN_FAILURE * 2):
            return "URGENT"
        if rate >= (config.MIN_FAILURE * 1.5):
            return "SEVERE"
        if rate >= config.MIN_FAILURE:
            return "CONCERN"
    return "OK"

def classify_volume_alert(deviation: float) -> str:
    """Classifies volume alerts based on percentage deviation."""
    if pd.isna(deviation):
        return "CAN'T EVALUATE"
    if deviation > config.MIN_VOLUME:
        return "NORMAL"
    if deviation > (config.MIN_VOLUME * 2):
        return "CONCERN"
    if deviation > (config.MIN_VOLUME * 4):
        return "SEVERE"
    return "URGENT"

def run_alerts(group_by_column: str, time_range: str) -> dict:
    """
    Executes the alert evaluation logic on the enriched summary.
    
    Args:
        group_by_column (str): Column used to group the data.
        time_range (str): Processed time range.

    Returns:
        dict: A dictionary containing DataFrames for 'failure', 'volume', 'version', and 'summary'.
              Returns None if the input file does not exist.
    """
    input_csv = f"{config.SUMMARY_DIR}{config.INPUT_FILES.get(group_by_column, group_by_column + '_')}{time_range}{config.SUMMARY_ENDING}"

    if not os.path.exists(input_csv):
        print(f"Enriched file {input_csv} not found. Ensure transformation ran correctly.")
        return None

    df = pd.read_csv(input_csv)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Apply Business Rules
    df["failure_alert"] = df.apply(
        lambda row: classify_failure_alert(row.get("failure_rate", np.nan), row.get("politics_search") == "Sí"),
        axis=1
    )
    df["volume_alert"] = df["volume_deviation"].apply(classify_volume_alert)

    # Filter Alert Subsets
    df_failure_alerts = df[df["failure_alert"].isin(["CONCERN", "SEVERE", "URGENT"])].copy()
    df_volume_alerts = df[df["volume_deviation"] <= config.MIN_VOLUME].copy()
    df_version_alerts = df[(df.get("current_version", 1) == 0) & (df.get("total_operations", 0) > 0)].copy()

    # Create Summary Metrics
    summary_data = {
        "Metric": [
            "Total Entities Analysed", 
            "Entities with Failure Alerts",
            "Entities with Volume Alerts", 
            "Invalid Entities (Version = 0)"
        ],
        "Value": [
            df[group_by_column].nunique(),
            len(df_failure_alerts),
            len(df_volume_alerts),
            len(df_version_alerts)
        ]
    }
    df_summary = pd.DataFrame(summary_data)

    print(f"Alerts evaluated: {len(df_failure_alerts)} failure, {len(df_volume_alerts)} volume.")

    return {
        "failure": df_failure_alerts,
        "volume": df_volume_alerts,
        "version": df_version_alerts,
        "summary": df_summary
    }
