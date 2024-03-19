import json
import logging
import os
import sys

import requests
from datetime import datetime


def is_validate_date(date_string: str) -> bool:
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True
    except ValueError:
        return False


# Create logger and set it to log into a .log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logs/ingest.log")
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

# Get date param from the command line
if len(sys.argv) != 2:
    logger.error("Usage: python ingest.py YYYYMMDD")
    print("Usage: python ingest.py YYYYMMDD")
    sys.exit(1)

date = sys.argv[1]
if not is_validate_date(date):
    logger.error("The date should be in the format YYYYMMDD")
    print("The date should be in the format YYYYMMDD")
    sys.exit(1)

# Create a directory structure to save data and logs
os.makedirs("data-raw", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# Get the data-raw from the API
url = f"https://api.covidtracking.com/v1/us/{date}.json"
response = requests.get(url)
data = response.json()

if data["error"]:
    logger.error(data["message"])
    print(data["message"])
    sys.exit(1)

# Save the data-raw to a file
filename = f"data-raw/{date}.json"
with open(filename, "w") as file:
    json.dump(data, file)

# Print the data-raw
print(data)
