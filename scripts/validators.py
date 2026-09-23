"""
validators.py
==============

Validation and cleaning helper functions for the
COVID-19 ETL Pipeline.
"""

from __future__ import annotations

from datetime import datetime


# ==========================================================
# STATE NAME STANDARDIZATION
# ==========================================================

STATE_ALIASES = {

    "andaman and nicobar islands":
        "Andaman and Nicobar Islands",

    "andaman & nicobar islands":
        "Andaman and Nicobar Islands",

    "dadra and nagar haveli and daman and diu":
        "Dadra and Nagar Haveli and Daman and Diu",

    "dadra & nagar haveli and daman & diu":
        "Dadra and Nagar Haveli and Daman and Diu",

    "orissa":
        "Odisha",

    "pondicherry":
        "Puducherry",

    "uttaranchal":
        "Uttarakhand",

    "jammu & kashmir":
        "Jammu and Kashmir",

    "nct of delhi":
        "Delhi"

}


# ==========================================================
# VALID DATE
# ==========================================================

def valid_date(value):

    if value is None:

        return False

    value = str(value).strip()

    if value == "":

        return False

    formats = [

        "%Y-%m-%d",

        "%d/%m/%Y",

        "%m/%d/%Y",

        "%d-%m-%Y",

        "%d-%b-%Y",

        "%d-%b-%y"

    ]

    for fmt in formats:

        try:

            datetime.strptime(value, fmt)

            return True

        except ValueError:

            pass

    return False


# ==========================================================
# CONVERT DATE TO ISO FORMAT
# ==========================================================

def iso_date(value):

    value = str(value).strip()

    formats = [

        "%Y-%m-%d",

        "%d/%m/%Y",

        "%m/%d/%Y",

        "%d-%m-%Y",

        "%d-%b-%Y",

        "%d-%b-%y"

    ]

    for fmt in formats:

        try:

            return datetime.strptime(

                value,

                fmt

            ).date().isoformat()

        except ValueError:

            pass

    return None


# ==========================================================
# VALID NUMBER
# ==========================================================

def valid_number(value):

    try:

        float(

            str(value)

            .replace(",", "")

            .strip()

        )

        return True

    except:

        return False


# ==========================================================
# CONVERT NUMBER
# ==========================================================

def number(value):

    if value is None:

        return 0

    value = (

        str(value)

        .replace(",", "")

        .strip()

    )

    if value.lower() in [

        "",

        "na",

        "n/a",

        "null",

        "none",

        "-"

    ]:

        return 0

    try:

        return int(float(value))

    except:

        return 0


# ==========================================================
# CLEAN STRING
# ==========================================================

def clean_string(value):

    if value is None:

        return ""

    return " ".join(

        str(value)

        .split()

    ).strip()


# ==========================================================
# CLEAN STATE
# ==========================================================

def clean_state(value):

    value = clean_string(value)

    if value == "":

        return ""

    key = value.lower()

    if key in STATE_ALIASES:

        return STATE_ALIASES[key]

    return value


# ==========================================================
# VALID STATE
# ==========================================================

def valid_state(value):

    if value is None:

        return False

    value = clean_state(value)

    if value == "":

        return False

    invalid = [

        "india",

        "all india",

        "total",

        "grand total"

    ]

    return value.lower() not in invalid


# ==========================================================
# REMOVE DUPLICATES
# ==========================================================

def remove_duplicates(rows):

    unique = []

    seen = set()

    for row in rows:

        key = tuple(

            sorted(row.items())

        )

        if key not in seen:

            seen.add(key)

            unique.append(row)

    return unique


# ==========================================================
# REMOVE EMPTY ROWS
# ==========================================================

def remove_empty_rows(rows):

    cleaned = []

    for row in rows:

        if any(

            str(value).strip()

            for value in row.values()

        ):

            cleaned.append(row)

    return cleaned


# ==========================================================
# VALID RECORD
# ==========================================================

def valid_record(row, required_fields):

    for field in required_fields:

        if field not in row:

            return False

        if str(row[field]).strip() == "":

            return False

    return True


# ==========================================================
# SAFE DIVISION
# ==========================================================

def safe_divide(a, b):

    if b == 0:

        return 0

    return a / b


# ==========================================================
# PERCENTAGE
# ==========================================================

def percentage(a, b):

    if b == 0:

        return 0

    return round(

        (a / b) * 100,

        2

    )


# ==========================================================
# SORT BY DATE
# ==========================================================

def sort_by_date(rows):

    return sorted(

        rows,

        key=lambda row: row["Date"]

    )


# ==========================================================
# SORT BY STATE
# ==========================================================

def sort_by_state(rows):

    return sorted(

        rows,

        key=lambda row: (

            row["State"],

            row["Date"]

        )

    )


# ==========================================================
# FIND MISSING VALUES
# ==========================================================

def missing_values(rows):

    count = 0

    for row in rows:

        for value in row.values():

            if str(value).strip() in [

                "",

                "NA",

                "N/A",

                "NULL",

                "None",

                "-"

            ]:

                count += 1

    return count


# ==========================================================
# COUNT DUPLICATES
# ==========================================================

def duplicate_count(rows):

    return len(rows) - len(

        remove_duplicates(rows)

    )


# ==========================================================
# DATE RANGE
# ==========================================================

def date_range(rows):

    dates = [

        row["Date"]

        for row in rows

    ]

    return (

        min(dates),

        max(dates)

    )


# ==========================================================
# UNIQUE STATES
# ==========================================================

def unique_states(rows):

    return sorted({

        row["State"]

        for row in rows

    })


# ==========================================================
# TOTAL RECORDS
# ==========================================================

def total_records(rows):

    return len(rows)


# ==========================================================
# IS POSITIVE NUMBER
# ==========================================================

def positive_number(value):

    return number(value) >= 0


# ==========================================================
# END OF FILE
# ==========================================================