import polars as pl
from pathlib import Path
import time

def process_recordings_data():
    pl.Config.set_tbl_width_chars(2000)     # Large enough to fit all columns
    pl.Config.set_tbl_cols(None)            # Disable column truncation
    pl.Config.set_tbl_rows(100)             # Show up to 100 rows

    handle_samples = True

    # Define input and output paths
    data_dir = Path("/data")
    input_recordings_file = data_dir / "recordings_anon_sample.csv" if handle_samples else data_dir / "recordings_anon.csv"
    input_identifications_file = data_dir / "species_ids_sample.csv" if handle_samples else data_dir / "species_ids.csv"
    output_file = data_dir / "observations_sample.parquet" if handle_samples else data_dir / "observations.parquet"

    # Load FinBIF species list, which is in the same directory as this script
    finbif_species_file = Path(__file__).parent / "species_list.csv"
    finbif_species_df = pl.read_csv(finbif_species_file, separator=";")

    try:
        start_time = time.time()
        print("Starting data processing...")

        # 1) Recordings - using lazy evaluation
        print("Processing recordings data...")
        recordings_df = pl.scan_csv(input_recordings_file)

        # Remove unneeded columns
        recordings_df = recordings_df.drop(["real_obs", "len", "dur"])

        # Add columns for year, month and day, by splitting date column, as integers
        recordings_df = recordings_df.with_columns([
            pl.col("date").str.split("-").list.get(0).cast(pl.Int32).alias("year"),
            pl.col("date").str.split("-").list.get(1).cast(pl.Int32).alias("month"),
            pl.col("date").str.split("-").list.get(2).cast(pl.Int32).alias("day")
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

        # 4) Add FinBIF species information using a join instead of map_elements
        print("Adding FinBIF species information...")
        # Convert finbif_species_df to lazy for better performance
        finbif_species_lazy = pl.LazyFrame(finbif_species_df)
        # Join with the species information
        joined_df = joined_df.join(
            finbif_species_lazy.select(["species", "finbif_species", "identifier"]),
            on="species",
            how="left"
        )

        # Recast year, month, and day to ensure they're Int32 before writing
        # Doesn't work, values are still floats, but they are integers in the data
        #joined_df = joined_df.with_columns([
        #    pl.col("year").cast(pl.Int32),
        #    pl.col("month").cast(pl.Int32),
        #    pl.col("day").cast(pl.Int32)
        #])

        joined_df = joined_df.with_columns([
            pl.col("year").cast(pl.Int32),
            pl.col("month").cast(pl.Int32),
            pl.col("day").cast(pl.Int32)
        ])

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
