"""
run_spark_pipeline.py
=====================

Master ETL Pipeline

Workflow

Raw CSV
    ↓
Load
    ↓
Transform
    ↓
Fact Tables
    ↓
Dimension Tables
    ↓
Quality Report
    ↓
Export CSV
"""

from pathlib import Path

from spark_config import create_spark, stop_spark

from spark_transform import (
    load_covid_data,
    load_vaccination_data,
    transform_covid_data,
    transform_vaccination_data,
    save_fact_tables
)

from spark_dimensions import (
    create_date_dimension,
    create_state_dimension,
    save_dimensions
)

from spark_quality import (
    run_quality_check
)


# ==========================================================
# PROJECT PATHS
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_FOLDER = PROJECT_ROOT / "data" / "raw"

PROCESSED_FOLDER = PROJECT_ROOT / "data" / "processed"

PROCESSED_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)

COVID_FILE = RAW_FOLDER / "covid_statewise_2020_2023.csv"

VACCINATION_FILE = RAW_FOLDER / "vaccination_statewise_2021_2023.csv"


# ==========================================================
# CHECK FILES
# ==========================================================

def check_input_files():

    print("\nChecking input files...")

    if not COVID_FILE.exists():
        raise FileNotFoundError(
            f"\nCOVID dataset not found:\n{COVID_FILE}"
        )

    print("✓ COVID dataset found")

    if not VACCINATION_FILE.exists():
        raise FileNotFoundError(
            f"\nVaccination dataset not found:\n{VACCINATION_FILE}"
        )

    print("✓ Vaccination dataset found")


# ==========================================================
# MAIN
# ==========================================================

def main():

    print("=" * 70)
    print("COVID-19 INDIA POWER BI DASHBOARD")
    print("MASTER ETL PIPELINE")
    print("=" * 70)

    spark = create_spark()

    try:

        # -----------------------------------
        # Check Input Files
        # -----------------------------------

        check_input_files()

        # -----------------------------------
        # Load Raw Data
        # -----------------------------------

        covid_raw = load_covid_data(
            spark,
            str(COVID_FILE)
        )

        vaccination_raw = load_vaccination_data(
            spark,
            str(VACCINATION_FILE)
        )

        # -----------------------------------
        # Transform
        # -----------------------------------

        print("\nTransforming datasets...")

        covid_fact = transform_covid_data(
            covid_raw
        )

        vaccination_fact = transform_vaccination_data(
            vaccination_raw
        )

        print("✓ Transformation completed")

        # -----------------------------------
        # Create Dimensions
        # -----------------------------------

        print("\nCreating dimensions...")

        date_dimension = create_date_dimension(
            covid_fact,
            vaccination_fact
        )

        state_dimension = create_state_dimension(
            covid_fact,
            vaccination_fact
        )

        # -----------------------------------
        # Quality Report
        # -----------------------------------

        print("\nRunning Data Quality Assessment...")

        run_quality_check(
            spark,
            covid_fact,
            vaccination_fact,
            str(PROCESSED_FOLDER)
        )

        # -----------------------------------
        # Save Fact Tables
        # -----------------------------------

        print("\nSaving Fact Tables...")

        save_fact_tables(

            covid_fact,

            vaccination_fact,

            str(PROCESSED_FOLDER)

        )

        # -----------------------------------
        # Save Dimensions
        # -----------------------------------

        print("\nSaving Dimensions...")

        save_dimensions(

            date_dimension,

            state_dimension,

            str(PROCESSED_FOLDER)

        )

        print()

        print("=" * 70)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print()

        print("Generated files")

        print("------------------------------")

        print("✓ covid_fact")

        print("✓ vaccination_fact")

        print("✓ date_dimension")

        print("✓ state_dimension")

        print("✓ quality_report")

        print("------------------------------")

    finally:

        stop_spark(spark)


# ==========================================================
# RUN
# ==========================================================

if __name__ == "__main__":
    main()