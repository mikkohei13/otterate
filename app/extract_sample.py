import polars as pl
import os

def extract_sample(input_file, output_file, n_rows=100000):
    """Extract n rows from a CSV file and save to a new file."""
    random_rows = False

    print(f"Processing {input_file}...")
    df = pl.read_csv(input_file)

    if random_rows:
        df = df.sample(n=n_rows)
    else:
        df = df.head(n_rows)

    df.write_csv(output_file)
    print(f"Saved {n_rows} rows to {output_file}")


def main():
    data_dir = "/data"
    files = ["species_ids.csv", "recordings_anon.csv"]
    
    for file in files:
        input_path = os.path.join(data_dir, file)
        output_path = os.path.join(data_dir, file.replace(".csv", "_sample.csv"))
        extract_sample(input_path, output_path)

if __name__ == "__main__":
    main()
