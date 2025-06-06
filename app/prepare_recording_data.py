import polars as pl
from pathlib import Path
import time

def process_recordings_data():
    pl.Config.set_tbl_width_chars(2000)     # Large enough to fit all columns
    pl.Config.set_tbl_cols(None)            # Disable column truncation
    pl.Config.set_tbl_rows(100)             # Show up to 100 rows

    # Define input and output paths
    data_dir = Path("/data")
    input_recordings_file = data_dir / "recordings_anon.csv"
    input_identifications_file = data_dir / "identifications.csv"
    output_file = data_dir / "observations.parquet"

    try:
        start_time = time.time()
        print("Starting data processing...")

        # 1) Recordings - using lazy evaluation
        print("Processing recordings data...")
        recordings_df = pl.scan_csv(input_recordings_file)
        
        # Convert 'len' to float and fill nulls
        recordings_df = recordings_df.with_columns([
            pl.col("len").fill_null(0.0).cast(pl.Float32)
        ])

        # 2) Species IDs - using lazy evaluation
        print("Processing species IDs data...")
        species_ids_df = pl.scan_csv(input_identifications_file)
        
        # Remove unneeded columns and filter before materializing
        species_ids_df = species_ids_df.drop(["orig_prediction", "feedback", "model_version"])
        species_ids_df = species_ids_df.filter(pl.col("prediction") >= 0.7)

        # 3) Join dataframes using rec_id field
        print("Joining datasets...")
        joined_df = recordings_df.join(species_ids_df, on="rec_id", how="right")

        # Materialize and save in chunks
        print("Saving to parquet...")
        joined_df.sink_parquet(output_file)
        
        end_time = time.time()
        print(f"Successfully processed data and saved to {output_file}")
        print(f"Processing took {end_time - start_time:.2f} seconds")

    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    process_recordings_data()
