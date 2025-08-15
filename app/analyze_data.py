# Loads data from parquet to a Pandas dataframe from a parquet file and shows some statistics

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import re

input_file = Path("/data/observations.parquet")


'''
The Polars dataframe contains bird observations identified by AI. It has the following columns:

user_anon', String, identifier of the user who made the recording
date', String, date of the recording (YYYY-MM-DD)
time', String, time of the recording (HH:MM:SS)
rec_type', String, type of the recording: direct, interval or point
point_count_loc', String, location name of the recording (usually empty)
lat', Float64, latitude of the recording
lon', Float64, longitude of the recording
url', String, URL of the recording file
year', Float64, year of the recording
month', Float64, month of the recording
day', Float64, day of the recording
species', String, scientific name of the bird species
prediction', Float64, prediction value between 0 and 1
song_start', Float64, start time of the song in the recording in seconds
rec_id', String, unique identifier of the recording
result_id', String, unique identifier of the result
isseen', Boolean, whether the bird was seen or not, can be empty
isheard', Boolean, whether the bird was heard or not, can be empty
finbif_species', String, scientific name of the bird species from FinBIF
identifier', String, identifier of the bird species from FinBIF
'''

# Define Finland boundaries
lat_min = 59.719384
lat_max = 70.095071
lon_min = 19.032174
lon_max = 31.587662

# Read only columns user_anon, finbif_species, month
df = pd.read_parquet(input_file, columns=["user_anon", "finbif_species", "lat", "lon", "prediction"])

# Remove rows where certain fields are empty
df = df.dropna(subset=["user_anon", "finbif_species", "lat", "lon"])

# Remove rows where prediction is <= 0.9
df = df[df['prediction'] > 0.9]

# Remove rows where lat or lon is outside Finland
df = df[
    (df['lat'] >= lat_min) & 
    (df['lat'] <= lat_max) & 
    (df['lon'] >= lon_min) & 
    (df['lon'] <= lon_max)
]

# Count number of rows
print(len(df))

# Save number of rows per species to a file
species_counts_file = Path("./output/species_counts_0.9.csv")
species_counts_file.parent.mkdir(parents=True, exist_ok=True)
df['finbif_species'].value_counts().to_csv(species_counts_file)

# Create a histogram of the prediction values
plt.hist(df['prediction'], bins=100)
plt.savefig("./output/prediction_histogram.png")
plt.close()



