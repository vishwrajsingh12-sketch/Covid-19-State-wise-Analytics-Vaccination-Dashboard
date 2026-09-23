"""
etl_pipeline.py
================

Main ETL Controller
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
    METADATA_FILE
)

from logger import logger

from profiling import profile_csv

from transformations import run_transformations

from dimensions import (
    create_date_dimension,
    create_state_dimension,
    save_date_dimension,
    save_state_dimension
)

from quality_report import (
    create_quality_report,
    save_quality_report
)


# ==========================================================
# CREATE METADATA
# ==========================================================

def create_metadata(

    covid_rows,

    vaccination_rows,

    date_dimension,

    state_dimension

):

    metadata = {

        "Project":
            "COVID-19 State-wise Analytics Dashboard",

        "Version":
            "1.0",

        "Generated":

            datetime.now()

            .strftime("%Y-%m-%d %H:%M:%S"),

        "COVID Records":
            len(covid_rows),

        "Vaccination Records":
            len(vaccination_rows),

        "Date Dimension":
            len(date_dimension),

        "State Dimension":
            len(state_dimension),

        "Status":
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

    logger.info(

        "Metadata created."

    )


# ==========================================================
# MAIN PIPELINE
# ==========================================================

def main():

    start = time.perf_counter()

    logger.info("=" * 70)

    logger.info(

        "COVID-19 ETL Pipeline Started"

    )

    logger.info("=" * 70)

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

    # -----------------------------------------------------
    # PROFILE RAW FILES
    # -----------------------------------------------------

    logger.info(

        "Profiling datasets"

    )

    covid_profile = profile_csv(

        COVID_FILE

    )

    vaccination_profile = profile_csv(

        VACCINATION_FILE

    )

    print()

    print("=" * 60)

    print("COVID DATASET")

    print("=" * 60)

    for key, value in covid_profile.items():

        print(f"{key}: {value}")

    print()

    print("=" * 60)

    print("VACCINATION DATASET")

    print("=" * 60)

    for key, value in vaccination_profile.items():

        print(f"{key}: {value}")

    # -----------------------------------------------------
    # CLEAN DATA
    # -----------------------------------------------------

    covid_rows, vaccination_rows = (

        run_transformations()

    )

    # -----------------------------------------------------
    # DIMENSIONS
    # -----------------------------------------------------

    logger.info(

        "Creating Date Dimension"

    )

    date_dimension = (

        create_date_dimension(

            covid_rows,

            vaccination_rows

        )

    )

    save_date_dimension(

        date_dimension

    )

    logger.info(

        "Creating State Dimension"

    )

    state_dimension = (

        create_state_dimension(

            covid_rows,

            vaccination_rows

        )

    )

    save_state_dimension(

        state_dimension

    )

    # -----------------------------------------------------
    # QUALITY REPORT
    # -----------------------------------------------------

    report = create_quality_report(

        covid_rows,

        vaccination_rows,

        date_dimension,

        state_dimension

    )

    save_quality_report(

        report

    )

    # -----------------------------------------------------
    # METADATA
    # -----------------------------------------------------

    create_metadata(

        covid_rows,

        vaccination_rows,

        date_dimension,

        state_dimension

    )

    elapsed = round(

        time.perf_counter()

        - start,

        2

    )

    logger.info("=" * 70)

    logger.info(

        "ETL Pipeline Completed"

    )

    logger.info(

        f"Execution Time : {elapsed} seconds"

    )

    logger.info("=" * 70)

    print()

    print("=" * 70)

    print("COVID-19 ETL PIPELINE COMPLETED")

    print("=" * 70)

    print(

        f"COVID Records            : {len(covid_rows)}"

    )

    print(

        f"Vaccination Records      : {len(vaccination_rows)}"

    )

    print(

        f"Date Dimension           : {len(date_dimension)}"

    )

    print(

        f"State Dimension          : {len(state_dimension)}"

    )

    print(

        f"Execution Time           : {elapsed} seconds"

    )

    print("=" * 70)


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    main()