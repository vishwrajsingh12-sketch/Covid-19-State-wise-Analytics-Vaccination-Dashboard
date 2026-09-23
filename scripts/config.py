"""
Project Configuration
"""

from pathlib import Path

# --------------------------------------------------
# ROOT
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --------------------------------------------------
# DATA
# --------------------------------------------------

DATA_FOLDER = PROJECT_ROOT / "data"

RAW_FOLDER = DATA_FOLDER / "raw"

CLEANED_FOLDER = DATA_FOLDER / "cleaned"

DIMENSION_FOLDER = DATA_FOLDER / "dimensions"

EXPORT_FOLDER = DATA_FOLDER / "exports"

# --------------------------------------------------
# DOCUMENTATION
# --------------------------------------------------

DOCS_FOLDER = PROJECT_ROOT / "docs"

LOG_FOLDER = PROJECT_ROOT / "logs"

# --------------------------------------------------
# INPUT FILES
# --------------------------------------------------

COVID_FILE = RAW_FOLDER / "covid_statewise_2020_2023.csv"

VACCINATION_FILE = RAW_FOLDER / "vaccination_statewise_2021_2023.csv"

# --------------------------------------------------
# OUTPUT FILES
# --------------------------------------------------

COVID_FACT = CLEANED_FOLDER / "covid_fact.csv"

VACCINATION_FACT = CLEANED_FOLDER / "vaccination_fact.csv"

DATE_DIMENSION = DIMENSION_FOLDER / "date_dimension.csv"

STATE_DIMENSION = DIMENSION_FOLDER / "state_dimension.csv"

QUALITY_REPORT = EXPORT_FOLDER / "quality_report.csv"

METADATA_FILE = EXPORT_FOLDER / "metadata.json"

# --------------------------------------------------
# LOG
# --------------------------------------------------

LOG_FILE = LOG_FOLDER / "etl.log"

# --------------------------------------------------
# PROJECT INFO
# --------------------------------------------------

PROJECT_NAME = "COVID-19 State-wise Analytics Dashboard"

VERSION = "1.0.0"

AUTHOR = "Purushottam"