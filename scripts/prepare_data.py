"""
prepare_data.py
================

Main ETL Controller

Workflow
--------
1. Profile raw datasets
2. Transform and clean datasets
3. Create dimension tables
4. Generate quality report
5. Generate metadata
"""

from __future__ import annotations

import json
import time
from datetime import datetime

from config import (
    COVID_FILE,
    VACCINATION_FILE,
    CLEANED_FOLDER,
    DIMENSION_FOLDER,
    EXPORT_FOLDER,
    METADATA_FILE,
)

from logger import logger

from profiling import profile_csv

from transformations import (
    run_transformations,
)

from dimensions import (
    create_date_dimension,
    create_state_dimension,
    save_date_dimension,
    save_state_dimension,
)

from quality_report import (
    create_quality_report,
    save_quality_report,
)


# ==========================================================
# METADATA
# ==========================================================

def create_metadata(
    covid_rows,
    vaccination_rows,
    date_dimension,
    state_dimension,
):
    """
    Create metadata.json
    """

    metadata = {

        "project": "COVID-19 State-wise Analytics Dashboard",

        "version": "1.0.0",

        "generated_on":
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

        "covid_records":
            len(covid_rows),

        "vaccination_records":
            len(vaccination_rows),

        "date_dimension_records":
            len(date_dimension),

        "state_dimension_records":
            len(state_dimension),

        "status":
            "Success"

    }

    EXPORT_FOLDER.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        METADATA_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            metadata,
            file,
            indent=4
        )

    logger.info("Metadata created.")


# ==========================================================
# MAIN ETL
# ==========================================================

def main():

    start = time.perf_counter()

    logger.info("=" * 70)
    logger.info("COVID-19 ETL Pipeline Started")
    logger.info("=" * 70)

    try:

        # --------------------------------------------------
        # Create Output Folders
        # --------------------------------------------------

        CLEANED_FOLDER.mkdir(
            parents=True,
            exist_ok=True
        )

        DIMENSION_FOLDER.mkdir(
            parents=True,
            exist_ok=True
        )

        EXPORT_FOLDER.mkdir(
            parents=True,
            exist_ok=True
        )

        logger.info("Output folders verified.")

        # --------------------------------------------------
        # PROFILE RAW DATASETS
        # --------------------------------------------------

        logger.info("Profiling datasets...")

        covid_profile = profile_csv(COVID_FILE)

        vaccination_profile = profile_csv(VACCINATION_FILE)

        print("\n" + "=" * 60)
        print("COVID DATASET PROFILE")
        print("=" * 60)

        for key, value in covid_profile.items():

            print(f"{key}: {value}")

        print("\n" + "=" * 60)
        print("VACCINATION DATASET PROFILE")
        print("=" * 60)

        for key, value in vaccination_profile.items():

            print(f"{key}: {value}")

        logger.info("Dataset profiling completed.")

        # --------------------------------------------------
        # CLEAN DATA
        # --------------------------------------------------

        logger.info("Running data transformation...")

        covid_rows, vaccination_rows = run_transformations()

        logger.info(
            f"COVID Records : {len(covid_rows)}"
        )

        logger.info(
            f"Vaccination Records : {len(vaccination_rows)}"
        )

        # --------------------------------------------------
        # CREATE DIMENSIONS
        # --------------------------------------------------

        logger.info("Creating Date Dimension...")

        date_dimension = create_date_dimension(
            covid_rows,
            vaccination_rows
        )

        save_date_dimension(
            date_dimension
        )

        logger.info(
            f"Date Dimension Rows : {len(date_dimension)}"
        )

        logger.info("Creating State Dimension...")

        state_dimension = create_state_dimension(
            covid_rows,
            vaccination_rows
        )

        save_state_dimension(
            state_dimension
        )

        logger.info(
            f"State Dimension Rows : {len(state_dimension)}"
        )

        # --------------------------------------------------
        # QUALITY REPORT
        # --------------------------------------------------

        logger.info("Generating Quality Report...")

        report = create_quality_report(

            covid_rows,

            vaccination_rows,

            date_dimension,

            state_dimension

        )

        save_quality_report(
            report
        )

        logger.info("Quality report saved.")

        # --------------------------------------------------
        # METADATA
        # --------------------------------------------------

        create_metadata(

            covid_rows,

            vaccination_rows,

            date_dimension,

            state_dimension

        )

        # --------------------------------------------------
        # FINISHED
        # --------------------------------------------------

        elapsed = round(

            time.perf_counter() - start,

            2

        )

        logger.info("=" * 70)
        logger.info("ETL Pipeline Completed Successfully")
        logger.info(f"Execution Time : {elapsed} seconds")
        logger.info("=" * 70)

        print("\n" + "=" * 70)
        print("COVID-19 ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(f"COVID Records              : {len(covid_rows)}")
        print(f"Vaccination Records        : {len(vaccination_rows)}")
        print(f"Date Dimension Records     : {len(date_dimension)}")
        print(f"State Dimension Records    : {len(state_dimension)}")
        print(f"Execution Time             : {elapsed} seconds")

        print("=" * 70)

    except Exception as error:

        logger.exception("ETL Pipeline Failed")

        print("\nETL PIPELINE FAILED\n")

        print(error)


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    main()