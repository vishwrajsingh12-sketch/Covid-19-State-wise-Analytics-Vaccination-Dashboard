"""
transformations.py
==================

Data transformation functions for the COVID-19 ETL Pipeline.
"""

from __future__ import annotations

import csv

from config import (
    COVID_FILE,
    VACCINATION_FILE,
    COVID_FACT,
    VACCINATION_FACT
)

from logger import logger

from validators import (
    valid_date,
    iso_date,
    valid_number,
    number,
    clean_state,
    valid_state,
    remove_duplicates,
    remove_empty_rows
)


# ==========================================================
# READ CSV
# ==========================================================

def read_csv(path):

    logger.info(f"Reading {path.name}")

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        return list(
            csv.DictReader(file)
        )


# ==========================================================
# WRITE CSV
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

    logger.info(f"Saved {path.name}")


# ==========================================================
# FIND COLUMN
# ==========================================================

def choose(headers, *options):

    lookup = {

        "".join(

            c.lower()

            for c in h

            if c.isalnum()

        ): h

        for h in headers

    }

    for option in options:

        key = "".join(

            c.lower()

            for c in option

            if c.isalnum()

        )

        if key in lookup:

            return lookup[key]

    raise ValueError(

        f"Column not found: {options}"

    )


# ==========================================================
# CLEAN COVID DATASET
# ==========================================================

def clean_covid():

    logger.info("Cleaning COVID dataset")

    rows = read_csv(COVID_FILE)

    headers = list(rows[0])

    date_col = choose(

        headers,

        "Date"

    )

    state_col = choose(

        headers,

        "State"

    )

    confirmed_col = choose(

        headers,

        "Confirmed",

        "Infected"

    )

    recovered_col = choose(

        headers,

        "Recovered"

    )

    deaths_col = choose(

        headers,

        "Deaths",

        "Death",

        "Deceased"

    )

    cleaned = []

    for row in rows:

        state = clean_state(

            row[state_col]

        )

        if not valid_state(state):

            continue

        if not valid_date(

            row[date_col]

        ):

            continue

        cleaned.append(

            {

                "Date":

                    iso_date(

                        row[date_col]

                    ),

                "State":

                    state,

                "Confirmed":

                    number(

                        row[confirmed_col]

                    ),

                "Recovered":

                    number(

                        row[recovered_col]

                    ),

                "Deaths":

                    number(

                        row[deaths_col]

                    )

            }

        )

    cleaned = remove_empty_rows(

        cleaned

    )

    cleaned = remove_duplicates(

        cleaned

    )

    cleaned = sorted(

        cleaned,

        key=lambda r: (

            r["Date"],

            r["State"]

        )

    )

    write_csv(

        COVID_FACT,

        [

            "Date",

            "State",

            "Confirmed",

            "Recovered",

            "Deaths"

        ],

        cleaned

    )

    logger.info(

        f"COVID dataset cleaned ({len(cleaned)} rows)"

    )

    return cleaned
# ==========================================================
# CLEAN VACCINATION DATASET
# ==========================================================

def clean_vaccination():

    logger.info("Cleaning Vaccination dataset")

    rows = read_csv(VACCINATION_FILE)

    headers = list(rows[0])

    date_col = choose(
        headers,
        "Vaccinated As Of",
        "Vaccinated.As.of",
        "Date"
    )

    state_col = choose(
        headers,
        "State"
    )

    total_col = choose(
        headers,
        "Total Doses Administered",
        "Total.Doses.Administered"
    )

    first_col = choose(
        headers,
        "First Dose Administered",
        "First.Dose.Administered"
    )

    second_col = choose(
        headers,
        "Second Dose Administered",
        "Second.Dose.Administered"
    )

    cleaned = []

    for row in rows:

        state = clean_state(
            row[state_col]
        )

        if not valid_state(state):
            continue

        if not valid_date(
            row[date_col]
        ):
            continue

        cleaned.append(

            {

                "Date":
                    iso_date(
                        row[date_col]
                    ),

                "State":
                    state,

                "Total Doses Administered":
                    number(
                        row[total_col]
                    ),

                "First Dose Administered":
                    number(
                        row[first_col]
                    ),

                "Second Dose Administered":
                    number(
                        row[second_col]
                    )

            }

        )

    cleaned = remove_empty_rows(
        cleaned
    )

    cleaned = remove_duplicates(
        cleaned
    )

    cleaned = sorted(

        cleaned,

        key=lambda r: (

            r["Date"],

            r["State"]

        )

    )

    write_csv(

        VACCINATION_FACT,

        [

            "Date",

            "State",

            "Total Doses Administered",

            "First Dose Administered",

            "Second Dose Administered"

        ],

        cleaned

    )

    logger.info(

        f"Vaccination dataset cleaned ({len(cleaned)} rows)"

    )

    return cleaned


# ==========================================================
# RUN COMPLETE TRANSFORMATION
# ==========================================================

def run_transformations():

    logger.info("=" * 60)

    logger.info("Running ETL Transformations")

    logger.info("=" * 60)

    covid_rows = clean_covid()

    vaccination_rows = clean_vaccination()

    logger.info("=" * 60)

    logger.info("Transformation Completed Successfully")

    logger.info("=" * 60)

    return (

        covid_rows,

        vaccination_rows

    )