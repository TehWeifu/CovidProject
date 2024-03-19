import json
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


# Get date param from the command line
date = sys.argv[1]
if not is_validate_date(date):
    print("The date should be in the format YYYYMMDD")
    sys.exit(1)

# Create a directory to store the data-raw
os.makedirs("data-raw", exist_ok=True)

# Get the data-raw from the API
url = f"https://api.covidtracking.com/v1/us/{date}.json"
response = requests.get(url)
data = response.json()

# Save the data-raw to a file
filename = f"data-raw/{date}.json"
with open(filename, "w") as file:
    json.dump(data, file)

# Print the data-raw
print(data)
