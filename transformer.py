"""
Data Transformation Module.
Loads operational data, aggregates booking flow information, merges historical data
and provider information, and exports a consolidated enriched CSV ready for evaluation.
"""

import os
import pandas as pd
import config

def calculate_period_days(dataframe: pd.DataFrame) -> int:
    """
    Calculates the number of real days covered by the dataset based on the @timestamp column.

    Args:
        dataframe (pd.DataFrame): Dataset of interest.
    
    Returns:
        int: Number of days covered in the dataset.
    """
    if "@timestamp" not in dataframe.columns:
        raise ValueError("@timestamp column is required to calculate period.")
    dataframe["@timestamp"] = pd.to_datetime(dataframe["@timestamp"])
    min_date = dataframe["@timestamp"].min()
    max_date = dataframe["@timestamp"].max()
    period_days = (max_date - min_date).days + 1
    return max(period_days, 1)

def build_base_summary(booking_flow_df: pd.DataFrame, group_by_column: str) -> pd.DataFrame:
    """
    Aggregates booking flow data by a designated column and computes basic operational metrics.

    Args:
        booking_flow_df (pd.DataFrame): Raw booking flow data.
        group_by_column (str): Column to group data by.

    Returns:
        pd.DataFrame: Aggregated summary.
    """
    df = booking_flow_df.copy()
    period_days = calculate_period_days(df)
    df["success"] = df["success"].astype(bool)

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
    summary["failure_rate"] = (summary["failed_operations"] / summary["total_operations"]) * 100
    total_failures = summary["failed_operations"].sum()
    summary["shared_rate"] = (summary["failed_operations"] / total_failures * 100) if total_failures else 0
    summary["daily_avg_operations"] = summary["total_operations"] / period_days

    return summary

def enrich_summary(df_current: pd.DataFrame, group_by_column: str) -> pd.DataFrame:
    """
    Merges the current summary with historical data and provider information,
    and calculates mathematical deviations.

    Args:
        df_current (pd.DataFrame): The base summary for the current period.
        group_by_column (str): Column used to group the data.

    Returns:
        pd.DataFrame: Enriched dataframe.
    """
    historic_csv = f"{config.HISTORIC_SUMMARY_DIR}{config.HISTORIC_FILES.get(group_by_column, 'historic_' + group_by_column)}{config.SUMMARY_ENDING}"
    df_historic = pd.read_csv(historic_csv) if os.path.exists(historic_csv) else pd.DataFrame()
    provider_info = pd.read_csv(config.PROVIDER_INFO_URL) if os.path.exists(config.PROVIDER_INFO_URL) else pd.DataFrame()

    df = df_current.merge(df_historic, on=group_by_column, how="left", suffixes=("", "_historic"))
    df = df.merge(provider_info, on=group_by_column, how="left")

    # Normalize binary flags for reporting
    flag_columns = ["politics_search", "loi", "mamushka"]
    for col in flag_columns:
        if col in df.columns:
            df[col] = df[col].map({1: "Sí", 0: "No", "1": "Sí", "0": "No"})

    # Calculate Volume Deviations
    if "daily_avg_operations_historic" in df.columns:
        df["volume_deviation"] = (
            ((df["daily_avg_operations"] - df["daily_avg_operations_historic"]) /
            df["daily_avg_operations_historic"]) * 100
        )
        df["volume_difference_absolute"] = (
            df["daily_avg_operations"] - df["daily_avg_operations_historic"]
        )
    else:
        df["volume_deviation"] = pd.NA
        df["volume_difference_absolute"] = pd.NA

    return df

def run_transformation(group_by_column: str, time_range: str, mode: str = "standard") -> None:
    """
    Executes the data transformation pipeline.
    
    Args:
        group_by_column (str): Column to aggregate by.
        time_range (str): The time range being processed.
        mode (str): Execution mode ('standard' or 'historic').
    """
    if mode == "historic":
        input_path = config.HISTORIC_DATA
        output_path = f"{config.HISTORIC_SUMMARY_DIR}{config.HISTORIC_FILES.get(group_by_column, 'historic_' + group_by_column)}{config.SUMMARY_ENDING}"
        print(f"Running HISTORIC transformation mode from {input_path} to {output_path}")
    else:
        input_path = f"{config.CURRENT_DATA}{time_range}_data.csv"
        # We save the enriched file back to the summary dir
        output_path = f"{config.SUMMARY_DIR}{config.INPUT_FILES.get(group_by_column, group_by_column + '_')}{time_range}{config.SUMMARY_ENDING}"
        print(f"Running STANDARD transformation mode from {input_path} to {output_path}")

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}. Skipping transformation.")
        return

    booking_flow_df = pd.read_csv(input_path)
    if booking_flow_df.empty:
        print("Input CSV is empty. Nothing to process.")
        return

    # 1. Build Base Summary
    summary_df = build_base_summary(booking_flow_df, group_by_column)

    # 2. If standard mode, enrich the summary with history and provider info
    if mode == "standard":
        summary_df = enrich_summary(summary_df, group_by_column)

    # 3. Export
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    summary_df.to_csv(output_path, index=False)
    print("Transformation completed successfully.")
