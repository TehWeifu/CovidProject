import json
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType

# Initialize SparkSession
spark = SparkSession.builder.appName("LocalizeCovidReport").getOrCreate()


# Define UDF for adjusting values with multi_factor
def adjust_value(value, multi_factor):
    if value is not None:
        return (value * multi_factor) * 10 // 10
    return None


adjust_value_udf = udf(adjust_value, DoubleType())

# Validate and get date param from the command line
try:
    date = sys.argv[1]
    datetime.strptime(date, '%Y%m%d')
except (IndexError, ValueError):
    print("The date should be provided and in the format YYYYMMDD.")
    sys.exit(1)

# Read cities data
cities_df = spark.read.parquet("data-source/cities.parquet")

# Read the report data
report_data = spark.read.json(f"./data-raw/{date}.json")

# Explode cities_df to have one row for each combination of city and the report
# This assumes you can expand the report identically for each city,
# which may require adjustments based on your exact data structure and needs.
reports_aggregated_df = cities_df.crossJoin(report_data.limit(1))

# Apply the multi_factor adjustment
numeric_columns = [col_name for col_name, dtype in report_data.dtypes if dtype in ['int', 'double', 'bigint']]
numeric_columns = [col_name for col_name in numeric_columns if col_name != "date"]
for col_name in numeric_columns:
    reports_aggregated_df = reports_aggregated_df.withColumn(
        col_name, adjust_value_udf(col(col_name), col("multi_factor"))
    )

# Add or modify necessary columns (city, country, multi_factor) as per original logic
# Assuming these columns exist or similar logic applies

# Save the results into a .parquet file
reports_aggregated_df.write.parquet(f"./data-localized/{date}.parquet")
print(f"File {date}.parquet created successfully")

# Move the original file to the processed folder
os.rename(f"./data-raw/{date}.json", f"./data-raw-processed/{date}.json")

# Stop SparkSession
spark.stop()
