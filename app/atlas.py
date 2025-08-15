# Script to compare observations to bird atlas results and save observations to a file, if they would be a new species for the square

import polars as pl
from pathlib import Path
from helpers.get_atlas_data import fetch_square_data, get_cached_square_data, read_bird_species_lookup
import json

# Configuration constants
SQUARE_DEBUG_LIMIT = 10000
OBSERVATION_MONTHS = (5, 7)
PREDICTION_THRESHOLD = 0.95
ATLAS_PREDICTION_THRESHOLD = 1.0
ALREADY_OBSERVED_VALUES = ["MY.atlasClassEnumA", "MY.atlasClassEnumB", "MY.atlasClassEnumC", "MY.atlasClassEnumD"]

# File paths
SQUARES_FILE = Path("./data/atlas_squares.csv")
OBSERVATION_DATA_FILE = Path("/data/observations_ykj.parquet")
PREDICTIONS_DIR = Path("./data/atlas_predictions_2024")

RESULTS_FILE = Path("./output/atlas_results.csv")


def load_and_filter_observations():
    """Load and pre-filter observation data from parquet file."""
    print("Loading and filtering observation data...")

    '''
    The Parquet file and the Polars dataframe contain bird observations identified by AI. They have the following columns:

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

    observations = pl.scan_parquet(OBSERVATION_DATA_FILE) \
        .select(["lat", "lon", "n", "e", "prediction", "month", "identifier", "rec_id", "result_id", "song_start", "isseen", "isheard", "date"]) \
        .filter(pl.col("month").is_between(*OBSERVATION_MONTHS)) \
        .filter(pl.col("prediction") >= PREDICTION_THRESHOLD) \
        .collect()
    
    print(f"Loaded {len(observations)} total observations")
    return observations


def get_already_observed_species(square_data):
    """Extract list of species already observed in a square."""
    already_observed_species = []
    
    for species in square_data["data"]:
        if species["atlasClass"] not in ALREADY_OBSERVED_VALUES:
            already_observed_species.append(species["speciesId"])
    
    return already_observed_species


def filter_observations_by_square(observations_df, ykj_n, ykj_e, already_observed_species):
    """Filter observations for a specific square and remove already observed species."""
    square_observations = observations_df.filter(pl.col("n") == ykj_n) \
        .filter(pl.col("e") == ykj_e)
    
    print(f"Number of observations: {len(square_observations)}")
    
    # Remove species already observed in the square
    filtered_observations = square_observations.filter(~pl.col("identifier").is_in(already_observed_species))
    
    return filtered_observations


def load_predictions_for_square(ykj_n, ykj_e):
    """Load prediction data for a specific square."""
    prediction_file = PREDICTIONS_DIR / f"{ykj_n}_{ykj_e}.json"
    
    try:
        with open(prediction_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: Prediction file not found for square {ykj_n}_{ykj_e}")
        return {}


def filter_by_atlas_predictions(observations_df, predictions, bird_species_lookup, square_info):
    """Filter observations based on atlas predictions and add metadata."""
    high_prediction_species = []
    
    for row in observations_df.iter_rows(named=True):
        identifier = row["identifier"]
        finnish_name = bird_species_lookup.get(identifier)
        
        if not finnish_name or finnish_name not in predictions:
            continue
            
        prediction_data = predictions[finnish_name]
        if not prediction_data or "predictions" not in prediction_data or not prediction_data["predictions"]:
            continue
            
        prediction_value = prediction_data["predictions"][0]["value"]
        
        if prediction_value >= ATLAS_PREDICTION_THRESHOLD:
            row_dict = dict(row)
            row_dict.update({
                "finnish_name": finnish_name,
                "atlas_prediction": round(prediction_value, 2),
                "square_name": square_info["name"],
                "activity_category": square_info["activity_category"],
                "bird_association_area": square_info["bird_association_area"]
            })
            high_prediction_species.append(row_dict)
    
    if high_prediction_species:
        high_prediction_df = pl.DataFrame(high_prediction_species)
        print(f"Number of species with prediction >= {ATLAS_PREDICTION_THRESHOLD}: {len(high_prediction_df)}")
        return high_prediction_df
    else:
        print(f"No species found with prediction >= {ATLAS_PREDICTION_THRESHOLD}")
        return create_empty_dataframe_with_schema(observations_df)


def create_empty_dataframe_with_schema(observations_df):
    """Create empty dataframe with extended schema for high prediction species."""
    schema_with_metadata = observations_df.schema.copy()
    schema_with_metadata.update({
        "finnish_name": pl.Utf8,
        "atlas_prediction": pl.Float64,
        "square_name": pl.Utf8,
        "activity_category": pl.Utf8,
        "bird_association_area": pl.Utf8
    })
    return pl.DataFrame(schema=schema_with_metadata)


def write_results_to_file(observations_df, is_first_iteration):
    """Write results to CSV file, with header only for first iteration."""
    if is_first_iteration:
        observations_df.write_csv(RESULTS_FILE, separator=";")
    else:
        with open(RESULTS_FILE, 'a') as f:
            observations_df.write_csv(f, separator=";", include_header=False)


def process_square(square_row, all_observations, bird_species_lookup):
    """Process a single square and return filtered observations."""
    ykj_n = square_row["ykj_n"]
    ykj_e = square_row["ykj_e"]
    square_name = square_row["square_name"]
    
    print(f"Processing square: {square_name} ({ykj_n}:{ykj_e})")
    
    # Get square data from atlas
    try:
        square_data = get_cached_square_data(ykj_n, ykj_e)
    except Exception as e:
        print(f"Error getting data for square {square_name}: {str(e)}")
        return None
    
    # Extract square information
    square_info = {
        "name": square_data["name"],
        "activity_category": square_data["activityCategory"]["value"],
        "bird_association_area": square_data["birdAssociationArea"]["value"]
    }
    
    # Get already observed species
    already_observed_species = get_already_observed_species(square_data)
    
    # Filter observations for this square
    square_observations = filter_observations_by_square(
        all_observations, ykj_n, ykj_e, already_observed_species
    )
    
    # Load predictions and filter by atlas predictions
    predictions = load_predictions_for_square(ykj_n, ykj_e)
    filtered_observations = filter_by_atlas_predictions(
        square_observations, predictions, bird_species_lookup, square_info
    )
    
    print(f"Number of observations after filtering: {len(filtered_observations)}")
    return filtered_observations


def main():
    """Main function to process all squares and generate results."""
    # Load atlas squares
    squares_df = pl.read_csv(SQUARES_FILE, separator=";")
    total_squares = len(squares_df)
    
    # Load bird species lookup
    bird_species_lookup = read_bird_species_lookup()
    
    # Load and pre-filter observations
    all_observations = load_and_filter_observations()
    
    # Process squares
    square_count = 0
    for i, square_row in enumerate(squares_df.iter_rows(named=True)):
        print(f"Processing {i+1}/{total_squares}: {square_row['square_name']}")
        
        filtered_observations = process_square(square_row, all_observations, bird_species_lookup)
        
        if filtered_observations is not None:
            # Write results to file
            write_results_to_file(filtered_observations, i == 0)
            square_count += 1
        
        # Check debug limit
        if square_count >= SQUARE_DEBUG_LIMIT:
            print(f"Debug limit reached, stopping at {square_count} squares")
            break
        
        print("--")


if __name__ == "__main__":
    main()


