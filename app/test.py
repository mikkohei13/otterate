# Export part of Parquet file to CSV for testing

import polars as pl
from pathlib import Path

input_file = Path("/data/observations_ykj_sample.parquet")

# Read the parquet file
df = pl.read_parquet(input_file)

# Print sample as CSV
output_file = Path("/data/output/observations_ykj_top100.csv")
df.head(100).write_csv(output_file)


