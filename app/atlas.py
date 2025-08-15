# Script to compare observations to bird atlas results and save observations to a file, if they would be a new species for the square

import polars as pl
from pathlib import Path
from helpers.get_atlas_data import fetch_square_data, get_cached_square_data, read_bird_species_lookup
import json

debug_limit = 100

# Load atlas squares
squares_file = Path("./data/atlas_squares.csv")
squares_df = pl.read_csv(squares_file, separator=";")

total_squares = len(squares_df)

already_observed_values = ["MY.atlasClassEnumA", "MY.atlasClassEnumB", "MY.atlasClassEnumC", "MY.atlasClassEnumD"]

# Load bird species lookup
bird_species_lookup = read_bird_species_lookup()


# Pre-filter the entire dataset once
observation_data_file = Path("/data/observations_ykj.parquet")
print("Loading and filtering observation data...")
all_observations = pl.scan_parquet(observation_data_file) \
    .select(["n", "e", "prediction", "month", "identifier", "rec_id", "result_id", "song_start", "isseen", "isheard"]) \
    .filter(pl.col("month").is_between(5, 7)) \
    .filter(pl.col("prediction") >= 0.95) \
    .collect()

print(f"Loaded {len(all_observations)} total observations")

# Open a file to write results continuously
results_file = Path("./output/atlas_results.csv")

# Loop through squares
square_count = 0
for i, row in enumerate(squares_df.iter_rows(named=True)):
    ykj_n = row["ykj_n"]
    ykj_e = row["ykj_e"]
    square_name = row["square_name"]
    
    print(f"Processing {i+1}/{total_squares}: {square_name} ({ykj_n}:{ykj_e})")
    
    # Get list of species already observed in this square
    try:
        square_data = get_cached_square_data(ykj_n, ykj_e)
    except Exception as e:
        print(f"Error: {str(e)}")
        continue

    square_species_list = square_data["data"]
    already_observed_species_list = []

    square_name = square_data["name"]
    activity_category = square_data["activityCategory"]["value"]
    bird_association_area = square_data["birdAssociationArea"]["value"]

    # Loop list of species already observed in this square
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

    # Get species predictions for this square and keep only species that have high enough prediction value
    prediction_file = f"./data/atlas_predictions_2024/{ykj_n}_{ykj_e}.json"
    with open(prediction_file, "r") as f:
        predictions = json.load(f)

    # Create a list to store species with high enough prediction values
    high_prediction_species = []
    
    # Loop through observations and check prediction values
    prediction_threshold = 1
    for row in observations_df.iter_rows(named=True):
        identifier = row["identifier"]
        
        # Get Finnish name from bird_species_lookup using identifier
        finnish_name = bird_species_lookup.get(identifier)
        
        if finnish_name and finnish_name in predictions:
            # Get prediction value from the predictions dictionary
            prediction_data = predictions[finnish_name]
            if prediction_data and "predictions" in prediction_data and len(prediction_data["predictions"]) > 0:
                prediction_value = prediction_data["predictions"][0]["value"]
                
                # Check if prediction value is prediction_threshold or more
                if prediction_value >= prediction_threshold:
                    # Add Finnish name and prediction value to the row data
                    row_dict = dict(row)
                    row_dict["finnish_name"] = finnish_name
                    row_dict["atlas_prediction"] = round(prediction_value, 2)
                    row_dict["square_name"] = square_name
                    row_dict["activity_category"] = activity_category
                    row_dict["bird_association_area"] = bird_association_area
                    high_prediction_species.append(row_dict)
    
    # Create a new dataframe with only high prediction species
    if high_prediction_species:
        high_prediction_df = pl.DataFrame(high_prediction_species)
        print(f"Number of species with prediction >= {prediction_threshold}: {len(high_prediction_df)}")
        
        # Replace observations_df with the filtered version
        observations_df = high_prediction_df
    else:
        print(f"No species found with prediction >= {prediction_threshold}")
        # Create empty dataframe with same schema plus finnish_name and atlas_prediction columns
        schema_with_finnish = observations_df.schema.copy()
        schema_with_finnish["finnish_name"] = pl.Utf8
        schema_with_finnish["atlas_prediction"] = pl.Float64
        schema_with_finnish["square_name"] = pl.Utf8
        schema_with_finnish["activity_category"] = pl.Utf8
        schema_with_finnish["bird_association_area"] = pl.Utf8
        observations_df = pl.DataFrame(schema=schema_with_finnish)

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

    square_count += 1
    if square_count >= debug_limit:
        print(f"Debug limit reached, stopping at {square_count} squares")
        break

    print("--")


