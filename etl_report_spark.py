from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, substring
import sys
import os
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.appName("ProcessReport").getOrCreate()

# Get date param from the command line
date = sys.argv[1]
if not datetime.strptime(date, '%Y%m%d'):
    print("The date should be in the format YYYYMMDD")
    sys.exit(1)

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

# Stop the Spark session
spark.stop()
