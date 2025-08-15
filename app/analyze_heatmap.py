# Loads data from parquet to a Pandas dataframe from a parquet file and shows some statistics

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from pathlib import Path
import re
import gc
import numpy as np

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

# Define Finland bounding box
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

# Load Finland borders from local Natural Earth data
world = gpd.read_file("./ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")
finland = world[world.NAME == "Finland"]

# Create hexagonal grid covering Finland
def create_hex_grid(bounds, hex_size=0.1):
    """Create hexagonal grid covering the given bounds"""
    from shapely.geometry import Polygon
    import math
    
    # Calculate grid dimensions
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    
    # Hexagon geometry
    hex_radius = hex_size / 2
    hex_width = hex_radius * 2
    hex_height = hex_radius * math.sqrt(3)
    
    # Calculate number of hexagons needed
    cols = int(width / hex_width) + 2
    rows = int(height / hex_height) + 2
    
    hexagons = []
    
    for row in range(rows):
        for col in range(cols):
            # Offset every other row
            x_offset = hex_width * 0.75 if row % 2 == 1 else 0
            
            center_x = bounds[0] + col * hex_width + x_offset
            center_y = bounds[1] + row * hex_height
            
            # Create hexagon vertices
            vertices = []
            for i in range(6):
                angle = i * math.pi / 3
                x = center_x + hex_radius * math.cos(angle)
                y = center_y + hex_radius * math.sin(angle)
                vertices.append((x, y))
            
            hex_polygon = Polygon(vertices)
            hexagons.append(hex_polygon)
    
    return gpd.GeoDataFrame(geometry=hexagons, crs="EPSG:4326")

# Create hexagonal grid covering Finland
finland_bounds = finland.total_bounds
hex_grid = create_hex_grid(finland_bounds, hex_size=0.2)  # Adjust hex_size as needed

# Convert observations to GeoDataFrame
observation_points = gpd.GeoDataFrame(
    df, 
    geometry=[Point(xy) for xy in zip(df.lon, df.lat)],
    crs="EPSG:4326"
)

# Count observations in each hexagon
hex_grid['observation_count'] = 0
for idx, hex_geom in hex_grid.iterrows():
    # Count points that intersect with this hexagon
    count = sum(observation_points.geometry.intersects(hex_geom.geometry))
    hex_grid.loc[idx, 'observation_count'] = count

# Filter hexagons that have observations and are within Finland
hex_grid = hex_grid[hex_grid['observation_count'] > 0]
hex_grid = gpd.clip(hex_grid, finland)

# Create the heatmap visualization
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))

# Plot 1: Hexagonal heatmap
hex_grid.plot(
    column='observation_count',
    ax=ax1,
    cmap='viridis',
    legend=True,
    legend_kwds={'label': 'Number of Observations', 'shrink': 0.8}
)

# Add Finland borders
finland.boundary.plot(ax=ax1, color='black', linewidth=1)

ax1.set_title('Bird Observation Density in Finland\n(Hexagonal Heatmap)', fontsize=16, fontweight='bold')
ax1.set_xlabel('Longitude')
ax1.set_ylabel('Latitude')
ax1.grid(True, alpha=0.3)

# Plot 2: Statistics and information
ax2.axis('off')
ax2.set_xlim(0, 1)
ax2.set_ylim(0, 1)

# Add statistics text
stats_text = f"""
BIRD OBSERVATION STATISTICS

Total Observations: {len(df):,}
Unique Species: {df['finbif_species'].nunique():,}
Unique Users: {df['user_anon'].nunique():,}
Active Hexagons: {len(hex_grid):,}

Top 5 Most Observed Species:
{df['finbif_species'].value_counts().head().to_string()}

Data Range:
Latitude: {df['lat'].min():.3f}° to {df['lat'].max():.3f}°
Longitude: {df['lon'].min():.3f}° to {df['lon'].max():.3f}°

Prediction Quality:
Mean: {df['prediction'].mean():.3f}
Std: {df['prediction'].std():.3f}
"""

ax2.text(0.05, 0.95, stats_text, transform=ax2.transAxes, 
         fontsize=12, verticalalignment='top', fontfamily='monospace',
         bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))

plt.tight_layout()

# Create output directory if it doesn't exist
output_dir = Path("./output")
output_dir.mkdir(exist_ok=True)

# Save the map
output_file = output_dir / "finland_bird_observations_heatmap.png"
plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
print(f"Map saved to: {output_file}")

# Also save a high-quality PDF version
pdf_file = output_dir / "finland_bird_observations_heatmap.pdf"
plt.savefig(pdf_file, bbox_inches='tight', facecolor='white')
print(f"PDF version saved to: {pdf_file}")

plt.show()

# Print summary statistics
print(f"\n=== BIRD OBSERVATION ANALYSIS ===")
print(f"Total observations: {len(df):,}")
print(f"Unique species: {df['finbif_species'].nunique():,}")
print(f"Unique users: {df['user_anon'].nunique():,}")
print(f"Active hexagons: {len(hex_grid):,}")
print(f"Mean observations per hexagon: {hex_grid['observation_count'].mean():.1f}")
print(f"Max observations in a hexagon: {hex_grid['observation_count'].max():,}")

# Show top species
print(f"\nTop 10 most observed species:")
print(df['finbif_species'].value_counts().head(10))

# Show top users
print(f"\nTop 10 most active users:")
print(df['user_anon'].value_counts().head(10))

# Clean up memory
del df, hex_grid, observation_points
gc.collect()

