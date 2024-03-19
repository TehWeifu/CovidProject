1. Create a git repository for the project and push it to GitHub.
2. Make a script that fetches JSON data from `api.covidtracting.com` by date and saves it to a .json file.
3. Make a script that reads the data from the .json file and does the following
    - Uses Spark (either sql or rdd)
    - Adds a random country (from a hardcoded list saved in parquet) to the data.
    - Adds a random city (from a hardcoded list saved in parquet) to the data.
    - Saves the data in a parquet file.
4. The original file is moved to a 'old' folder.
5. Make an ETL (pentaho or python) to add process date, year, month and day to the parquet file and append it to an
   historical.
6. Using Power BI or make a report that shows the data by country and city.
