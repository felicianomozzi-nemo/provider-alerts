"""
Main entry point for the Operational Data Pipeline.
Orchestrates data extraction, transformation, alert logic, and reporting sequentially.
"""

import argparse
from extractor import run_extraction
from transformer import run_transformation
from alerter import run_alerts
from reporter import run_reporting

def main() -> None:
    """
    Main execution flow.
    Parses command line arguments and runs the entire data pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Run the full booking flow data pipeline."
    )
    parser.add_argument(
        "time_range",
        nargs="?",
        default="now-7d",
        help="Time range for data extraction (default: now-7d). Or pass 'historic'."
    )
    parser.add_argument(
        "group_by_column",
        nargs="?",
        default="provider_name",
        help="Column to group data by (default: provider_name)."
    )

    args = parser.parse_args()
    mode = "historic" if args.time_range == "historic" else "standard"

    print("=" * 70)
    print("🚀 STARTING PIPELINE")
    print(f"Mode: {mode.upper()}")
    print(f"Time Range: {args.time_range}")
    print(f"Grouping By: {args.group_by_column}")
    print("=" * 70)

    # 1. Extraction (extractor.py)
    print("\n--- STEP 1: EXTRACTION ---")
    run_extraction(time_range=args.time_range, mode=mode)

    # 2. Transformation & Enrichment (transformer.py)
    print("\n--- STEP 2: TRANSFORMATION & ENRICHMENT ---")
    run_transformation(group_by_column=args.group_by_column, time_range=args.time_range, mode=mode)

    # 3 & 4. Alerting and Reporting (Standard mode only)
    if mode == "standard":
        print("\n--- STEP 3: BUSINESS LOGIC (ALERTS) ---")
        alerts_data = run_alerts(group_by_column=args.group_by_column, time_range=args.time_range)
        if alerts_data:
            print("\n--- STEP 4: REPORTING (EXCEL GENERATION) ---")
            run_reporting(alerts_dict=alerts_data, group_by_column=args.group_by_column, time_range=args.time_range)
        else:
            print("\n⚠️ Skipping reporting due to missing data from alerting step.")
    else:
        print("\n--- Historic mode selected, skipping alert report generation ---")

    print("\n" + "=" * 70)
    print("✅ PIPELINE FINISHED SUCCESSFULLY")
    print("=" * 70)

if __name__ == "__main__":
    main()
