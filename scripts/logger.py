"""
Logging Configuration
"""

import logging

from config import LOG_FILE

LOG_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)

logging.basicConfig(

    filename=LOG_FILE,

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s",

    filemode="w"

)

logger = logging.getLogger("COVID_ETL")