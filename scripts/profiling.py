"""
profiling.py

Dataset profiling utilities for the ETL pipeline.
"""

from __future__ import annotations

import csv
from pathlib import Path
from collections import Counter


def profile_csv(file_path: Path) -> dict:
    """
    Profile a CSV file and return summary statistics.
    """

    with file_path.open(
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = list(csv.DictReader(file))

    if not reader:

        raise ValueError(
            f"{file_path.name} contains no data."
        )

    headers = reader[0].keys()

    duplicate_rows = len(reader) - len(
        {
            tuple(row.items())
            for row in reader
        }
    )

    missing = 0

    for row in reader:

        for value in row.values():

            if value.strip() in (
                "",
                "NA",
                "N/A",
                "NULL",
                "-"
            ):

                missing += 1

    return {

        "File": file_path.name,

        "Rows": len(reader),

        "Columns": len(headers),

        "Missing Values": missing,

        "Duplicate Rows": duplicate_rows,

        "Column Names": list(headers)

    }