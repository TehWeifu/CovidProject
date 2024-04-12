import logging
import sys
from datetime import datetime

from src.aggregate_localization import aggregate_localization
from src.etl_report import transform_load_report
from src.ingest import ingest_data


def main():
    logger = initialize_logger()
    date = get_date_param()
    if not date:
        logger.error("The date should be provided and in the format YYYYMMDD.")

    if not ingest_data(date, logger):
        logger.error("Data ingestion failed.")
        return

    if not aggregate_localization(date, logger):
        logger.error("Data aggregation failed.")
        return

    if not transform_load_report(date, logger):
        logger.error("Transform and load failed.")
        return

    logger.info("Process completed successfully.")


def get_date_param() -> str:
    try:
        date = sys.argv[1]
        datetime.strptime(date, '%Y%m%d')
    except (IndexError, ValueError):
        return ''
    return date


def initialize_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO)

    logger = logging.Logger("main")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler("logs/main.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


if __name__ == "__main__":
    main()
