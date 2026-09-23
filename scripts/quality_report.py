"""
quality_report.py
=================

Generate data quality report for the ETL pipeline.
"""

from __future__ import annotations

import csv

from config import QUALITY_REPORT

from logger import logger


# ==========================================================
# CSV WRITER
# ==========================================================

def write_csv(path, headers, rows):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=headers
        )

        writer.writeheader()

        writer.writerows(rows)


# ==========================================================
# QUALITY REPORT
# ==========================================================

def create_quality_report(

    covid_rows,

    vaccination_rows,

    date_dimension,

    state_dimension

):

    logger.info("Creating Quality Report")

    report = [

        {

            "Metric": "COVID Records",

            "Value": len(covid_rows)

        },

        {

            "Metric": "Vaccination Records",

            "Value": len(vaccination_rows)

        },

        {

            "Metric": "Date Dimension Records",

            "Value": len(date_dimension)

        },

        {

            "Metric": "State Dimension Records",

            "Value": len(state_dimension)

        },

        {

            "Metric": "Total States",

            "Value": len(state_dimension)

        },

        {

            "Metric": "COVID Date Range",

            "Value":

                f"{covid_rows[0]['Date']}"

                "  →  "

                f"{covid_rows[-1]['Date']}"

        },

        {

            "Metric": "Vaccination Date Range",

            "Value":

                f"{vaccination_rows[0]['Date']}"

                "  →  "

                f"{vaccination_rows[-1]['Date']}"

        },

        {

            "Metric": "Maximum Confirmed Cases",

            "Value":

                max(

                    row["Confirmed"]

                    for row in covid_rows

                )

        },

        {

            "Metric": "Maximum Recovered Cases",

            "Value":

                max(

                    row["Recovered"]

                    for row in covid_rows

                )

        },

        {

            "Metric": "Maximum Deaths",

            "Value":

                max(

                    row["Deaths"]

                    for row in covid_rows

                )

        },

        {

            "Metric": "Maximum Vaccine Doses",

            "Value":

                max(

                    row["Total Doses Administered"]

                    for row in vaccination_rows

                )

        }

    ]

    return report


# ==========================================================
# SAVE REPORT
# ==========================================================

def save_quality_report(report):

    logger.info("Saving Quality Report")

    write_csv(

        QUALITY_REPORT,

        [

            "Metric",

            "Value"

        ],

        report

    )

    logger.info("Quality Report Saved")