import polars as pl
from pathlib import Path

def process_recordings_data():
    pl.Config.set_tbl_width_chars(2000)     # Large enough to fit all columns
    pl.Config.set_tbl_cols(None)            # Disable column truncation
    pl.Config.set_tbl_rows(100)             # Show up to 100 rows

    # Define input and output paths
    data_dir = Path("/data")
    input_file = data_dir / "recordings_anon_sample.csv"
    output_file = data_dir / "recordings_anon_sample.parquet"
    sample_file = data_dir / "sample_output.csv"

    try:
        # Read CSV file
        df = pl.read_csv(input_file)

        # Convert 'real_obs' from string to boolean and replace empty 'len' values with 0.0
        df = df.with_columns([
            pl.col("len").fill_null(0.0)
        ])

        # Print sample of the dataframe
        print(df.head(10))
        print(df.schema)

        # Save as Parquet
        df.write_parquet(output_file)
        print(f"Successfully processed data and saved to {output_file}")

    except FileNotFoundError:
        print(f"Error: Input file {input_file} not found")
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    process_recordings_data()
