# Script to unify the historic data parquet files for PowerBI

import pandas as pd

# Read the parquet file
df = pd.read_parquet('./data-source/covid.parquet')

# Save the DataFrame to a parquet file
df.to_parquet('./data-source/covid_aggregated.parquet')
