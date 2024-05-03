import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession

from src.aggregate_localization import aggregate_localization
from src.etl_report import transform_load_report
from src.ingest import ingest_data


def main():
    logger = initialize_logger()

    spark = SparkSession.builder.appName("ProcessReport").getOrCreate()

    date_range = get_date_range_param()
    if not date_range:
        logger.error("The date should be provided and in the format YYYYMMDD.")

    for date in date_range:
        if not ingest_data(date, logger):
            logger.error(f"Data ingestion failed. ({date})")
            break

        if not aggregate_localization(date, spark, logger):
            logger.error(f"Data aggregation failed. ({date})")
            break

        if not transform_load_report(date, spark, logger):
            logger.error(f"Transform and load failed. ({date})")
            break

    spark.stop()

    logger.info("Process completed successfully.")


def get_date_range_param() -> list:
    try:
        if len(sys.argv) == 2:
            date = sys.argv[1]
            datetime.strptime(date, '%Y%m%d')
            return [date]
        elif len(sys.argv) == 3:
            start_date = sys.argv[1]
            end_date = sys.argv[2]
            datetime.strptime(start_date, '%Y%m%d')
            datetime.strptime(end_date, '%Y%m%d')
            return [str(date) for date in range(int(start_date), int(end_date) + 1)]
        else:
            return []
    except (IndexError, ValueError):
        return []


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
