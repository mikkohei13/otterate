# Script to compare observations to bird atlas results and save observations to a file, if they would be a new species for the square

import polars as pl
from pathlib import Path
from helpers.get_atlas_data import fetch_square_data, get_cached_square_data

# Load atlas squares
squares_file = Path("./data/atlas_squares.csv")
squares_df = pl.read_csv(squares_file, separator=";")

total_squares = len(squares_df)

already_observed_values = ["MY.atlasClassEnumB", "MY.atlasClassEnumC", "MY.atlasClassEnumD"]

# Pre-filter the entire dataset once - much faster than scanning 3859 times
observation_data_file = Path("/data/observations_ykj.parquet")
print("Loading and filtering observation data...")
all_observations = pl.scan_parquet(observation_data_file) \
    .select(["n", "e", "prediction", "month", "identifier", "rec_id", "result_id"]) \
    .filter(pl.col("month").is_between(5, 7)) \
    .filter(pl.col("prediction") >= 0.98) \
    .collect()

print(f"Loaded {len(all_observations)} total observations")

# Open a file to write results continuously
results_file = Path("./output/atlas_results.csv")

for i, row in enumerate(squares_df.iter_rows(named=True)):
    ykj_n = row["ykj_n"]
    ykj_e = row["ykj_e"]
    square_name = row["square_name"]
    
    print(f"Processing {i+1}/{total_squares}: {square_name} ({ykj_n}:{ykj_e})")
    
    try:
        square_data = get_cached_square_data(ykj_n, ykj_e)
    except Exception as e:
        print(f"Error: {str(e)}")
        continue

    # Get list of species already observed in this square
    square_species_list = square_data["data"]
    already_observed_species_list = []

    # Loop list of species
    for species in square_species_list:
        if species["atlasClass"] in already_observed_values:
            pass
        else:
            already_observed_species_list.append(species["speciesId"])


    '''
    The Polars dataframe contains bird observations identified by AI. It has the following columns:

    'user_anon', String, identifier of the user who made the recording
    'date', String, date of the recording (YYYY-MM-DD)
    'time', String, time of the recording (HH:MM:SS)
    'rec_type', String, type of the recording: direct, interval or point
    'point_count_loc', String, location name of the recording (usually empty)
    'lat', Float64, latitude of the recording
    'lon', Float64, longitude of the recording
    'url', String, URL of the recording file
    'year', Float64, year of the recording
    'month', Float64, month of the recording
    'day', Float64, day of the recording
    'species', String, scientific name of the bird species
    'prediction', Float64, prediction value between 0 and 1
    'song_start', Float64, start time of the song in the recording in seconds
    'rec_id', String, unique identifier of the recording
    'result_id', String, unique identifier of the result
    'isseen', Boolean, whether the bird was seen or not, can be empty
    'isheard', Boolean, whether the bird was heard or not, can be empty
    'finbif_species', String, scientific name of the bird species from FinBIF
    'identifier', String, identifier of the bird species from FinBIF
    'n', Integer, Finnish uniform grid system (ykj) EPSG:2393 column
    'e', Integer, Finnish uniform grid system (ykj) EPSG:2393 row
    '''

    # Filter from pre-loaded data instead of scanning file
    observations_df = all_observations.filter(pl.col("n") == ykj_n) \
        .filter(pl.col("e") == ykj_e)

    # Print number of observations
    print(f"Number of observations: {len(observations_df)}")

    # Print number of observations per species
#    print(observations_df.group_by("finbif_species").count())

    # Remove from observations_df those species that are already observed in the square
    observations_df = observations_df.filter(~pl.col("identifier").is_in(already_observed_species_list))

    # Print number of observations
    print(f"Number of observations after filtering: {len(observations_df)}")

    # Print number of observations per species
#    print(observations_df.group_by("finbif_species").count())

    # Append rows to results file
    if i == 0:
        # Write header for first iteration
        observations_df.write_csv(results_file, separator=";")
    else:
        # Append without header for subsequent iterations
        with open(results_file, 'a') as f:
            observations_df.write_csv(f, separator=";", include_header=False)

    print("--")


