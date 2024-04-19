# Script to unify the historic data parquet files for PowerBI

import pandas as pd

df = pd.read_parquet('./../data-source/covid.parquet')
df.to_parquet('./../data-source/covid_unified.parquet')
