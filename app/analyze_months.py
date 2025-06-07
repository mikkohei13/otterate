# Loads data to a Pandas dataframe from a parquet file and shows some statistics

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import re

input_file = Path("/data/observations.parquet")

# Print schema of the dataframe
#print(pd.read_parquet(input_file).dtypes)
#exit()

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
# Read only columns user_anon, finbif_species, month
df = pd.read_parquet(input_file, columns=["user_anon", "finbif_species", "month"])

# Remove rows where certain fields are empty
df = df.dropna(subset=["user_anon", "finbif_species", "month"])

# Cast month to integer
df["month"] = df["month"].astype(int)

# Function to create safe filename from species name
def safe_filename(species):
    # Replace spaces and special characters with underscores
    return re.sub(r'[^a-zA-Z0-9]', '_', species)

# Group by species and month, then create a chart for each species
for species in df["finbif_species"].unique():
    # Filter data for this species
    species_data = df[df["finbif_species"] == species]
    
    # Group by month and count
    monthly_counts = species_data.groupby("month").size().reindex(range(1, 13), fill_value=0).reset_index(name="count")
    monthly_counts = monthly_counts.sort_values("month")
    
    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.bar(monthly_counts["month"], monthly_counts["count"])
    plt.title(f"Monthly observations of {species}")
    plt.xlabel("Month")
    plt.ylabel("Number of observations")
    plt.xticks(range(1, 13))
    
    # Save the chart with species name in filename
    safe_species_name = safe_filename(species)
    plt.savefig(f"/data/output/monthly_{safe_species_name}.png")
    plt.close()

# Save as csv for debugging purposes
#df.to_csv("/data/output/observations_sample.csv")
#exit()

