import pandas as pd
import os

def extract_sample(input_file, output_file, n_rows=100):
    """Extract first n rows from a CSV file and save to a new file."""
    print(f"Processing {input_file}...")
    df = pd.read_csv(input_file, nrows=n_rows)
    df.to_csv(output_file, index=False)
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
