# Loads data to a Pandas dataframe from a parquet file and shows some statistics

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from pathlib import Path
import re
import gc

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

# Load Finland borders from local Natural Earth data
world = gpd.read_file("./ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")
finland = world[world.NAME == "Finland"]

# Create output directory if it doesn't exist
output_dir = Path("/data/output")
output_dir.mkdir(parents=True, exist_ok=True)

# Define Finland boundaries
lat_min = 59.719384
lat_max = 70.095071
lon_min = 19.032174
lon_max = 31.587662

# Get unique species list first
species_list = pd.read_parquet(input_file, columns=["finbif_species"])["finbif_species"].unique()

# Process each species
for species in species_list:
    # Read only data for current species
    species_df = pd.read_parquet(
        input_file,
        columns=["user_anon", "finbif_species", "lat", "lon", "month"],
        filters=[("finbif_species", "==", species)]
    )
    
    # Filter for summer months
    species_df = species_df[species_df['month'].isin([5, 6, 7])]
    
    # Filter coordinates
    species_df = species_df[
        (species_df['lat'] >= lat_min) & 
        (species_df['lat'] <= lat_max) & 
        (species_df['lon'] >= lon_min) & 
        (species_df['lon'] <= lon_max)
    ]
    
    # Skip if no observations for this species
    if len(species_df) == 0:
        continue
    
    # Remove rows where lat or lon is empty
    species_df = species_df.dropna(subset=["user_anon", "lat", "lon"])
    
    # Convert lat and lon to float
    species_df["lat"] = species_df["lat"].astype(float)
    species_df["lon"] = species_df["lon"].astype(float)
    
    # Convert to GeoDataFrame
    geometry = [Point(xy) for xy in zip(species_df['lon'], species_df['lat'])]
    gdf = gpd.GeoDataFrame(species_df, geometry=geometry, crs="EPSG:4326")
    
    # Create a map of Finland with the points
    plt.figure(figsize=(10, 10))
    ax = finland.plot(color='white', edgecolor='black')
    gdf.plot(ax=ax, markersize=2, color='red')
    plt.title(f"Observations of {species}")
    plt.axis('off')
    
    # Create filename from species name (replace spaces and special characters)
    safe_species_name = re.sub(r'[^a-zA-Z0-9]', '_', species)
    output_file = output_dir / f"map_{safe_species_name}.png"
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close('all')  # Close all figures
    
    # Clear memory
    del species_df, gdf, geometry
    gc.collect()