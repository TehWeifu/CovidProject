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

# Read the report .parquet file
report = pd.read_parquet(f"./data-localized/{date}.parquet")

# Add processed date to the report with the current date
report["processed_date"] = datetime.now()

# Get the year from the first 4 gits fo the date column
report["collected_year"] = report["date"].astype(str).str[:4]
report['collected_month'] = report["date"].astype(str).str[4:6]
report['collected_day'] = report["date"].astype(str).str[6:8]

# Append the dataframe to a historical .parquet file
if os.path.exists("data-source/report.parquet"):
    historical_report = pd.read_parquet("data-source/covid.parquet")
    historical_report = historical_report.append(report)
    historical_report.to_parquet("./data-source/covid.parquet")
else:
    report.to_parquet("./data-source/covid.parquet")

# Move the localized file to the processed folder
os.rename(f"./data-localized/{date}.parquet", f"./data-localized-processed/{date}.parquet")
