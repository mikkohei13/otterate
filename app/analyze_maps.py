# Loads data to a Pandas dataframe from a parquet file and shows some statistics

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from pathlib import Path
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

# Load Finland borders from local Natural Earth data
world = gpd.read_file("./ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")
finland = world[world.NAME == "Finland"]

# Read only columns user_anon, finbif_species, month
df = pd.read_parquet(input_file, columns=["user_anon", "finbif_species", "lat", "lon"])

# Filter out rows where coordinates are outside Finland
lat_min = 59.719384
lat_max = 70.095071
lon_min = 19.032174
lon_max = 31.587662

# Filter out rows where coordinates are outside Finland
df = df[(df['lat'] >= lat_min) & (df['lat'] <= lat_max) & (df['lon'] >= lon_min) & (df['lon'] <= lon_max)]

# Filter out rows where finbif_species is not "Oriolus oriolus"
# Select only species "Oriolus oriolus"
df = df[df["finbif_species"] == "Oriolus oriolus"]

# Remove rows where lat or lon is empty
df = df.dropna(subset=["user_anon", "lat", "lon"])

# Convert lat and lon to float
df["lat"] = df["lat"].astype(float)
df["lon"] = df["lon"].astype(float)

# Convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# Create a map of Finland with the points
output_file = Path("/data/output/map.png")
ax = finland.plot(color='white', edgecolor='black')
gdf.plot(ax=ax, markersize=2, color='red')
plt.axis('off')
plt.savefig(output_file, bbox_inches='tight', dpi=300)
plt.close()