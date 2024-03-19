import json
import sys
from datetime import datetime
import pandas as pd
import os

# Get date param from the command line
date = sys.argv[1]
if not datetime.strptime(date, '%Y%m%d'):
    print("The date should be in the format YYYYMMDD")
    sys.exit(1)

# Read the cities .parquet file
cities = pd.read_parquet("data-source/cities.parquet")

results = []
# Read the report .json file
with open(f"./data-raw/{date}.json") as covid_report:
    report = json.load(covid_report)

    # Loop through the cities dataframe
    for index, row in cities.iterrows():
        # Create a copy of the report
        report_copy = report.copy()
        # Add the city to the report
        report_copy["city"] = row["city_id"]
        report_copy["country"] = row["country_id"]
        # Transform the report into a dictionary
        results.append(report_copy)

# Save the results into a .parquet file
df_results = pd.DataFrame(results)
df_results.to_parquet(f"./data-localized/{date}.parquet")
print(f"File {date}.parquet created successfully")

# Move the original file to the processed folder
os.rename(f"./data-raw/{date}.json", f"./data-processed/{date}.json")
