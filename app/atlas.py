# Script to compare observations to bird atlas results and save interesting observations to a file

import polars as pl
from pathlib import Path
from helpers.get_atlas_data import fetch_square_data, get_cached_square_data

# Load atlas squares
squares_file = Path("./data/atlas_squares.csv")
squares_df = pl.read_csv(squares_file, separator=";")

total_squares = len(squares_df)

already_observed_values = ["MY.atlasClassEnumB", "MY.atlasClassEnumC", "MY.atlasClassEnumD"]

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

    observation_data_file = Path("/data/observations_ykj.parquet")

    # Note: the Parquet file is very large, so code needs to be optimized for reading only what is needed
    # Read data of this specific square, and where month is 3...8 (March to August)
#    observations_df = pl.read_parquet(observation_data_file, columns=["n", "e", "lat", "lon", "date", "time", "month", "prediction", "finbif_species", "identifier", "rec_id", "result_id"])
    observations_df = pl.read_parquet(observation_data_file, columns=["n", "e", "month", "prediction", "finbif_species", "identifier", "rec_id", "result_id"])
    observations_df = observations_df.filter(pl.col("n") == ykj_n)
    observations_df = observations_df.filter(pl.col("e") == ykj_e)
    observations_df = observations_df.filter(pl.col("month").is_between(4, 7))
    observations_df = observations_df.filter(pl.col("prediction") >= 0.95)

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


