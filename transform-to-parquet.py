# Date to process from cmd

import sys
from datetime import datetime
from pyspark.sql import SparkSession

# Get date param from the command line
date = sys.argv[1]
if not datetime.strptime(date, '%Y%m%d'):
    print("The date should be in the format YYYYMMDD")
    sys.exit(1)

# Create a Spark session
spark = SparkSession.builder.appName("CovidData").getOrCreate()

# Load the data-raw from the JSON file
df = spark.read.json(f"data/{date}.json")

# Save the data-raw to a Parquet file
df.write.mode("overwrite").parquet(f"parquet/{date}")

# Stop the Spark session
spark.stop()
