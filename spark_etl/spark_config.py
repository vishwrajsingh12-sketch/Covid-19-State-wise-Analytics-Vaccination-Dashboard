"""
spark_config.py
===============================

Spark Configuration Module

Creates a Spark Session for the
COVID-19 India Power BI Dashboard ETL Pipeline.
Compatible with PySpark 4.x
"""

from pyspark.sql import SparkSession


APP_NAME = "COVID19 India PowerBI Dashboard"


def create_spark():
    """
    Create Spark Session
    """

    spark = (

        SparkSession.builder

        .appName(APP_NAME)

        .master("local[*]")

        # -------------------------------
        # Driver & Executor Memory
        # -------------------------------

        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")

        # -------------------------------
        # Performance
        # -------------------------------

        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")

        # -------------------------------
        # Date Handling
        # -------------------------------

        .config(
            "spark.sql.legacy.timeParserPolicy",
            "LEGACY"
        )

        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )

        # IMPORTANT
        # Disable ANSI exceptions so mixed
        # date formats won't crash Spark.

        .config(
            "spark.sql.ansi.enabled",
            "false"
        )

        # -------------------------------
        # Adaptive Query Execution 
        # -------------------------------

        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )

        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        # -------------------------------
        # Arrow
        # -------------------------------

        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "true"
        )

        .getOrCreate()

    )

    spark.sparkContext.setLogLevel("ERROR")

    print()

    print("=" * 70)
    print("SPARK SESSION CREATED")
    print("=" * 70)

    print(
        "Application Name :",
        spark.sparkContext.appName
    )

    print(
        "Spark Version    :",
        spark.version
    )

    print(
        "Master           :",
        spark.sparkContext.master
    )

    print(
        "Default Parallel :",
        spark.sparkContext.defaultParallelism
    )

    print("=" * 70)

    return spark


def stop_spark(spark):
    """
    Stop Spark Session
    """

    if spark:

        spark.stop()

        print()

        print("=" * 70)
        print("Spark Session Closed")
        print("=" * 70)