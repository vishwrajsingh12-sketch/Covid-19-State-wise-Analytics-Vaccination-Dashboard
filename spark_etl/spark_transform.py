"""
spark_transform.py
==================

PySpark Transformation Module

Responsibilities
----------------
1. Load raw datasets
2. Clean datasets
3. Transform datasets
4. Create Fact Tables
5. Export Fact Tables
"""
import os

from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    coalesce,
    to_date,
    lit
)

from pyspark.sql.types import IntegerType


# ==========================================================
# LOAD DATASETS
# ==========================================================

def load_covid_data(spark, filepath):

    print("\nLoading COVID dataset...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(filepath)
    )

    print("COVID Rows :", df.count())

    return df


def load_vaccination_data(spark, filepath):

    print("\nLoading Vaccination dataset...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(filepath)
    )

    print("Vaccination Rows :", df.count())

    return df


# ==========================================================
# STANDARDIZE STATE NAMES
# ==========================================================

def standardize_state_names(df):

    replacements = {

        "Orissa": "Odisha",

        "Pondicherry": "Puducherry",

        "Andaman & Nicobar Islands":
        "Andaman and Nicobar Islands",

        "Dadra & Nagar Haveli":
        "Dadra and Nagar Haveli",

        "Daman & Diu":
        "Daman and Diu"

    }

    df = df.withColumn(
        "State",
        trim(col("State"))
    )

    return df.replace(
        replacements,
        subset=["State"]
    )


# ==========================================================
# CLEAN NUMERIC COLUMNS
# ==========================================================

def clean_numeric_columns(df, columns):

    for column in columns:

        if column in df.columns:

            df = (

                df

                .withColumn(
                    column,
                    regexp_replace(
                        col(column).cast("string"),
                        ",",
                        ""
                    )
                )

                .withColumn(
                    column,
                    regexp_replace(
                        col(column),
                        " ",
                        ""
                    )
                )

                .withColumn(
                    column,
                    col(column).cast(IntegerType())
                )

            )

    return df


# ==========================================================
# DATE PARSER
# Handles:
# 15-11-2022
# 14/4/2022
# 10/3/2020
# 2022-11-15
# ==========================================================

def parse_date(column_name):

    return coalesce(

        to_date(col(column_name), "dd-MM-yyyy"),

        to_date(col(column_name), "d-M-yyyy"),

        to_date(col(column_name), "dd/MM/yyyy"),

        to_date(col(column_name), "d/M/yyyy"),

        to_date(col(column_name), "yyyy-MM-dd")

    )


# ==========================================================
# EXPORT CSV
# ==========================================================



import os


def export_csv(df, output_folder, filename):

    os.makedirs(output_folder, exist_ok=True)

    pdf = df.toPandas()

    pdf.to_csv(
        os.path.join(output_folder, filename),
        index=False
    )

    print(f"✓ {filename} exported")

# ==========================================================
# COVID TRANSFORMATION
# ==========================================================

def transform_covid_data(df):
    """
    Transform COVID dataset into Fact Table
    """

    print("Transforming COVID dataset...")

    # -----------------------------
    # Standardize State Names
    # -----------------------------
    df = standardize_state_names(df)

    # -----------------------------
    # Parse Date
    # -----------------------------
    df = df.withColumn(
        "Date",
        parse_date("Date")
    )

    # Remove invalid dates
    df = df.filter(col("Date").isNotNull())

    # -----------------------------
    # Clean Numeric Columns
    # -----------------------------
    numeric_columns = [
        "Infected",
        "Recovered",
        "Death"
    ]

    df = clean_numeric_columns(df, numeric_columns)

    # -----------------------------
    # Rename Columns
    # -----------------------------
    df = (
        df
        .withColumnRenamed("Infected", "Confirmed")
        .withColumnRenamed("Death", "Deaths")
    )

    # -----------------------------
    # Replace Null Numeric Values
    # -----------------------------
    df = (
        df
        .fillna(
            0,
            subset=[
                "Confirmed",
                "Recovered",
                "Deaths"
            ]
        )
    )

    # -----------------------------
    # Remove Duplicate Records
    # -----------------------------
    df = df.dropDuplicates(
        ["Date", "State"]
    )

    covid_fact = df.select(
        "Date",
        "State",
        "Confirmed",
        "Recovered",
        "Deaths"
    )

    print("✓ COVID Transformation Completed")

    return covid_fact
# ==========================================================
# VACCINATION TRANSFORMATION
# ==========================================================

def transform_vaccination_data(df):
    """
    Transform Vaccination dataset
    """

    print("Transforming Vaccination dataset...")

    df = standardize_state_names(df)

    # -----------------------------
    # Detect Date Column
    # -----------------------------
    if "Vaccinated As of" in df.columns:

        date_column = "Vaccinated As of"

    elif "Date" in df.columns:

        date_column = "Date"

    else:

        raise Exception("Date column not found.")

    # -----------------------------
    # Parse Date
    # -----------------------------
    df = df.withColumn(
        "Date",
        parse_date(date_column)
    )

    if date_column != "Date":
        df = df.drop(date_column)

    df = df.filter(
        col("Date").isNotNull()
    )

    # -----------------------------
    # Clean Numeric Columns
    # -----------------------------
    numeric_columns = [
        "First Dose Administered",
        "Second Dose Administered",
        "Total Doses Administered"
    ]

    df = clean_numeric_columns(
        df,
        numeric_columns
    )

    df = df.fillna(
        0,
        subset=numeric_columns
    )

    df = df.dropDuplicates(
        ["Date", "State"]
    )

    vaccination_fact = df.select(
        "Date",
        "State",
        "First Dose Administered",
        "Second Dose Administered",
        "Total Doses Administered"
    )

    print("✓ Vaccination Transformation Completed")

    return vaccination_fact

# ==========================================================
# SAVE FACT TABLES
# ==========================================================

def save_fact_tables(
    covid_df,
    vaccination_df,
    output_folder
):

    export_csv(
        covid_df,
        output_folder,
        "covid_fact.csv"
    )

    export_csv(
        vaccination_df,
        output_folder,
        "vaccination_fact.csv"
    )

    print()
    print("=" * 60)
    print("FACT TABLES CREATED")
    print("=" * 60)
    print("✓ covid_fact")
    print("✓ vaccination_fact")
    print("=" * 60)

    # ==========================================================
# SHOW SUMMARY
# ==========================================================

def show_summary(
    covid_df,
    vaccination_df
):

    print()

    print("=" * 60)
    print("COVID FACT TABLE")
    print("=" * 60)

    print("Rows :", covid_df.count())
    print("Columns :", len(covid_df.columns))

    covid_df.show(5, truncate=False)

    print()

    print("=" * 60)
    print("VACCINATION FACT TABLE")
    print("=" * 60)

    print("Rows :", vaccination_df.count())
    print("Columns :", len(vaccination_df.columns))

    vaccination_df.show(5, truncate=False)

    print("=" * 60)