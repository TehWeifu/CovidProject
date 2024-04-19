import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, substring


def transform_load_report(date: str, spark: SparkSession, logger: logging.Logger) -> bool:
    # Define the path for the new report and the historical data
    report_path = f"./data-localized/{date}.parquet"
    historical_data_path = "./data-source/covid.parquet"

    # Read the report .parquet file
    report_df = spark.read.parquet(report_path)

    # Add processed date to the report with the current date
    report_df = report_df.withColumn("processed_date", current_date())

    # Extract year, month, day from the date column
    report_df = report_df.withColumn("collected_year", substring("date", 1, 4))
    report_df = report_df.withColumn("collected_month", substring("date", 5, 2))
    report_df = report_df.withColumn("collected_day", substring("date", 7, 2))

    # Append the DataFrame to a historical .parquet file if it exists, otherwise create a new one
    if os.path.exists(historical_data_path):
        historical_report_df = spark.read.parquet(historical_data_path)
        combined_df = historical_report_df.unionByName(report_df, allowMissingColumns=True)
        combined_df.write.mode('overwrite').parquet(historical_data_path)
    else:
        report_df.write.parquet(historical_data_path)

    # Move the localized file to the processed folder
    processed_path = f"./data-localized-processed/{date}.parquet"
    os.rename(report_path, processed_path)

    return True
