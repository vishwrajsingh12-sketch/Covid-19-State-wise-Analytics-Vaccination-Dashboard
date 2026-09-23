"""
spark_quality.py
================

PySpark Data Quality Module

Creates quality metrics for:

1. COVID Fact Table
2. Vaccination Fact Table

Exports:
quality_report.csv
"""

import os
import pandas as pd

from pyspark.sql.functions import col


# ==========================================================
# ROW COUNT
# ==========================================================

def get_row_count(df):
    return df.count()


# ==========================================================
# COLUMN COUNT
# ==========================================================

def get_column_count(df):
    return len(df.columns)


# ==========================================================
# DUPLICATE COUNT
# ==========================================================

def get_duplicate_count(df):
    return df.count() - df.distinct().count()


# ==========================================================
# NULL COUNT
# ==========================================================

def get_null_count(df):

    total = 0

    for c in df.columns:
        total += df.filter(col(c).isNull()).count()

    return total


# ==========================================================
# CREATE QUALITY REPORT
# ==========================================================

def create_quality_report(df, dataset_name):

    return {

        "Dataset": dataset_name,

        "Rows": get_row_count(df),

        "Columns": get_column_count(df),

        "Duplicates": get_duplicate_count(df),

        "Null Values": get_null_count(df)

    }


# ==========================================================
# PRINT REPORT
# ==========================================================

def print_report(report):

    print()

    print("=" * 60)
    print(report["Dataset"])
    print("=" * 60)

    print("Rows        :", report["Rows"])
    print("Columns     :", report["Columns"])
    print("Duplicates  :", report["Duplicates"])
    print("Null Values :", report["Null Values"])


# ==========================================================
# SAVE QUALITY REPORT
# ==========================================================

def save_quality_report(

    covid_report,

    vaccination_report,

    output_folder

):

    os.makedirs(output_folder, exist_ok=True)

    report = pd.DataFrame([
        covid_report,
        vaccination_report
    ])

    report.to_csv(
        os.path.join(output_folder, "quality_report.csv"),
        index=False
    )

    print()
    print("✓ quality_report.csv exported")


# ==========================================================
# RUN QUALITY CHECK
# ==========================================================

def run_quality_check(

    spark,

    covid_df,

    vaccination_df,

    output_path

):

    print()

    print("=" * 60)
    print("DATA QUALITY REPORT")
    print("=" * 60)

    covid_report = create_quality_report(
        covid_df,
        "COVID FACT"
    )

    vaccination_report = create_quality_report(
        vaccination_df,
        "VACCINATION FACT"
    )

    print_report(covid_report)
    print_report(vaccination_report)

    # Save report
    save_quality_report(

        covid_report,

        vaccination_report,

        output_path

    )

    return covid_report, vaccination_report