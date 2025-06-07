# Loads data to a Polars dataframe from a parquet file and shows some statistics

import polars as pl
from pathlib import Path
import matplotlib.pyplot as plt
#import pandas as pd

input_file = Path("/data/observations_sample.parquet")

df = pl.read_parquet(input_file)

# Remove rows where user_anon is empty
df = df.filter(pl.col("user_anon").is_not_null())

'''
The Polars dataframe contains bird observations identified by AI. It has the following columns:

user_anon', String, identifier of the user who made the recording
date', String, date of the recording (YYYY-MM-DD)
time', String, time of the recording (HH:MM:SS)
len', Float32, length of the recording in seconds
dur', Float64, duration of the recording in seconds
rec_type', String, type of the recording: direct, interval or point
point_count_loc', String, location name of the recording (usually empty)
lat', Float64, latitude of the recording
lon', Float64, longitude of the recording
url', String, URL of the recording file
year', Int32, year of the recording (YYYY)
month', Int32, month of the recording (MM)
day', Int32, day of the recording (DD)
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

# Save as csv for debugging purposes
df.write_csv("/data/output/observations_sample.csv")
exit()

'''
# Find observations Of a single species, and save them to csv
df_species = df.filter(pl.col("finbif_species") == "Parus major")
df_species.write_csv("/data/output/observations_species.csv")

# Make a chart showing the number of observations per month for the species
# Convert Polars DataFrame to Pandas
pdf_species = df_species.to_pandas()

# Group by month and count
monthly_counts = pdf_species.groupby('month').size().reset_index(name='count')

# Plot
monthly_counts.plot(x='month', y='count', kind='bar', legend=False, title='Monthly count of Parus major')
plt.xlabel('Month')
plt.ylabel('Number of observations')
plt.tight_layout()
# Save the chart to a file
plt.savefig("/data/output/observations_species_monthly.png")
plt.close()

# Show how many rows there are per "species", print to csv
#df.group_by("species").count().write_csv("/data/output/species_counts.csv")

# Show how many rows there are per "user_anon", but only for top 10 users, print to csv
#print(df.group_by("user_anon").count().sort("count", descending=True).head(10))

'''

