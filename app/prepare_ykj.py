# Script to add Finnish uniform grid system EPSG:2393 coordinates to a parquet file, based on WGS84 decimal degrees (lat & lon fields)

import polars as pl
from pyproj import Transformer

# Read the parquet file
df = pl.read_parquet("/data/observations_sample.parquet")

# Remove rows where either lat or lon is empty
df = df.filter(pl.col("lat").is_not_null() & pl.col("lon").is_not_null())

# Create a transformer from WGS84 (EPSG:4326) to ETRS89 / TM35FIN (EPSG:2393)
transformer = Transformer.from_crs("EPSG:4326", "EPSG:2393", always_xy=True)

# Convert coordinates in batches using vectorized operations
x, y = transformer.transform(
    df["lon"].to_numpy(),
    df["lat"].to_numpy()
)

# Add the new columns to the dataframe using Polars expressions
df = df.with_columns([
    (pl.lit(y) / 10000).floor().cast(pl.Int32).alias("n"),
    (pl.lit(x) / 10000).floor().cast(pl.Int32).alias("e")
])

# Save as parquet
df.write_parquet("/data/observations_ykj_sample.parquet")
