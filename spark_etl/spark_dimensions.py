"""
spark_dimensions.py
===================

PySpark Dimension Module

Creates:

1. Date Dimension
2. State Dimension

Exports both dimensions to CSV.
"""
import os

from pyspark.sql.functions import (
    col,
    year,
    quarter,
    month,
    weekofyear,
    dayofmonth,
    dayofweek,
    date_format,
    when
)


# ==========================================================
# DATE DIMENSION
# ==========================================================

def create_date_dimension(
    covid_df,
    vaccination_df
):
    """
    Create Date Dimension
    """

    print("\nCreating Date Dimension...")

    dates = (

        covid_df

        .select("Date")

        .union(

            vaccination_df.select("Date")

        )

        .distinct()

        .orderBy("Date")

    )

    date_dimension = (

        dates

        .withColumn(
            "Year",
            year(col("Date"))
        )

        .withColumn(
            "Quarter",
            quarter(col("Date"))
        )

        .withColumn(
            "Month Number",
            month(col("Date"))
        )

        .withColumn(
            "Month",
            date_format(
                col("Date"),
                "MMMM"
            )
        )

        .withColumn(
            "Month Short",
            date_format(
                col("Date"),
                "MMM"
            )
        )

        .withColumn(
            "Week Number",
            weekofyear(col("Date"))
        )

        .withColumn(
            "Day",
            dayofmonth(col("Date"))
        )

        .withColumn(
            "Day Name",
            date_format(
                col("Date"),
                "EEEE"
            )
        )

        .withColumn(
            "Day Number",
            dayofweek(col("Date"))
        )

        .withColumn(

            "Weekend",

            when(

                dayofweek(col("Date")).isin(1, 7),

                "Yes"

            ).otherwise(

                "No"

            )

        )

    )

    print("✓ Date Dimension")

    return date_dimension


# ==========================================================
# STATE DIMENSION
# ==========================================================

def create_state_dimension(
    covid_df,
    vaccination_df
):
    """
    Create State Dimension
    """

    print("\nCreating State Dimension...")

    state_dimension = (

        covid_df

        .select("State")

        .union(

            vaccination_df.select("State")

        )

        .distinct()

        .orderBy("State")

    )

    print("✓ State Dimension")

    return state_dimension


# ==========================================================
# EXPORT CSV
# ==========================================================



def export_dimension(df, output_folder, filename):

    os.makedirs(output_folder, exist_ok=True)

    pdf = df.toPandas()

    pdf.to_csv(
        os.path.join(output_folder, filename),
        index=False
    )

    print(f"✓ {filename} exported")



# ==========================================================
# SAVE DIMENSIONS
# ==========================================================

def save_dimensions(
    date_dimension,
    state_dimension,
    output_folder
):
    """
    Save Date & State Dimension
    """

    export_dimension(
    date_dimension,
    output_folder,
    "date_dimension.csv"
    )

    export_dimension(
        state_dimension,
        output_folder,
        "state_dimension.csv"
    )

    print()

    print("=" * 60)
    print("DIMENSIONS CREATED")
    print("=" * 60)

    print("✓ date_dimension")

    print("✓ state_dimension")

    print("=" * 60)


# ==========================================================
# SUMMARY
# ==========================================================

def show_dimension_summary(
    date_dimension,
    state_dimension
):
    """
    Display Summary
    """

    print()

    print("=" * 60)
    print("DATE DIMENSION")
    print("=" * 60)

    print("Rows :", date_dimension.count())
    print("Columns :", len(date_dimension.columns))

    date_dimension.show(
        5,
        truncate=False
    )

    print()

    print("=" * 60)
    print("STATE DIMENSION")
    print("=" * 60)

    print("Rows :", state_dimension.count())
    print("Columns :", len(state_dimension.columns))

    state_dimension.show(
        10,
        truncate=False
    )

    print("=" * 60)