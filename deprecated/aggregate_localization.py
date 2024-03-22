import json
import sys
from datetime import datetime
from math import ceil

import pandas as pd
import os


def aggregate_localization(report, city):
    report_copy = report.copy()

    # Multiply every numeric value by the multi_factor
    for key, value in report_copy.items():
        if key == "date" or key == "states":
            continue

        if isinstance(value, int):
            report_copy[key] = ceil(value * city["multi_factor"])
        elif isinstance(value, float):
            report_copy[key] = value * city["multi_factor"]

    return report_copy


# Get date param from the command line
date = sys.argv[1]
if not datetime.strptime(date, '%Y%m%d'):
    print("The date should be in the format YYYYMMDD")
    sys.exit(1)

# Read the cities .parquet file
cities = pd.read_parquet("../data-source/cities.parquet")

results = []
# Read the report .json file
with open(f"./data-raw/{date}.json") as covid_report:
    raw_report = json.load(covid_report)

    # Loop through the cities dataframe
    for index, row in cities.iterrows():
        city_report = aggregate_localization(raw_report, row)

        # Add the city to the report
        city_report["city"] = row["city_id"]
        city_report["country"] = row["country_id"]
        city_report["multi_factor"] = row["multi_factor"]

        # Transform the report into a dictionary
        results.append(city_report)

# Save the results into a .parquet file
df_results = pd.DataFrame(results)
df_results.to_parquet(f"./data-localized/{date}.parquet")
print(f"File {date}.parquet created successfully")

# Move the original file to the processed folder
os.rename(f"./data-raw/{date}.json", f"./data-raw-processed/{date}.json")
