"""
dimensions.py
==============

Create and export dimension tables for Power BI.
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta

from config import (
    DATE_DIMENSION,
    STATE_DIMENSION,
)

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
# DATE DIMENSION
# ==========================================================

def create_date_dimension(
    covid_rows,
    vaccination_rows
):

    logger.info("Creating Date Dimension")

    dates = []

    for row in covid_rows:

        dates.append(
            datetime.fromisoformat(
                row["Date"]
            ).date()
        )

    for row in vaccination_rows:

        dates.append(
            datetime.fromisoformat(
                row["Date"]
            ).date()
        )

    start = min(dates)

    end = max(dates)

    rows = []

    current = start

    while current <= end:

        rows.append({

            "Date":
                current.isoformat(),

            "Year":
                current.year,

            "Quarter":
                f"Q{((current.month-1)//3)+1}",

            "Month Number":
                current.month,

            "Month":
                current.strftime("%B"),

            "Month Short":
                current.strftime("%b"),

            "Week":
                current.isocalendar().week,

            "Day":
                current.day,

            "Day Name":
                current.strftime("%A"),

            "Day Short":
                current.strftime("%a"),

            "Weekend":

                "Yes"

                if current.weekday() >= 5

                else "No"

        })

        current += timedelta(days=1)

    return rows


# ==========================================================
# STATE DIMENSION
# ==========================================================

def create_state_dimension(
    covid_rows,
    vaccination_rows
):

    logger.info("Creating State Dimension")

    states = sorted({

        row["State"]

        for row in covid_rows + vaccination_rows

    })

    rows = []

    for index, state in enumerate(states, start=1):

        rows.append({

            "State ID":
                index,

            "State":
                state

        })

    return rows


# ==========================================================
# SAVE DATE DIMENSION
# ==========================================================

def save_date_dimension(rows):

    logger.info("Saving Date Dimension")

    write_csv(

        DATE_DIMENSION,

        [

            "Date",

            "Year",

            "Quarter",

            "Month Number",

            "Month",

            "Month Short",

            "Week",

            "Day",

            "Day Name",

            "Day Short",

            "Weekend"

        ],

        rows

    )


# ==========================================================
# SAVE STATE DIMENSION
# ==========================================================

def save_state_dimension(rows):

    logger.info("Saving State Dimension")

    write_csv(

        STATE_DIMENSION,

        [

            "State ID",

            "State"

        ],

        rows

    )