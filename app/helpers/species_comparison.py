# Script that compares species names by this app and FinBIF, and matches synonyms

import polars as pl

mlk_species_file = "/data/output/mlk_species.csv"
finbif_species_file = "/data/output/finbif_species.tsv"

# Read both files with their correct delimiters
mlk_species_df = pl.read_csv(mlk_species_file, separator=";")
finbif_species_df = pl.read_csv(finbif_species_file, separator="\t")

# Create a mapping dictionary from scientific name to FinBIF species name
finbif_mapping = dict(zip(finbif_species_df["Scientific name"], finbif_species_df["Scientific name"]))

# Prepare synonym search: list of (synonyms, scientific name)
synonym_rows = list(zip(finbif_species_df["Synonyms"], finbif_species_df["Scientific name"]))

def find_finbif_species(species_name):
    # 1. Direct match
    if species_name in finbif_mapping:
        return finbif_mapping[species_name]
    # 2. Search in synonyms
    for synonyms, sci_name in synonym_rows:
        if isinstance(synonyms, str) and species_name in synonyms:
            return sci_name
    # 3. No match
    return None

mlk_species_df = mlk_species_df.with_columns(
    pl.col("species").map_elements(find_finbif_species, return_dtype=pl.Utf8).alias("finbif_species")
)

# Create a mapping dictionary from scientific name to Identifier
identifier_mapping = dict(zip(finbif_species_df["Scientific name"], finbif_species_df["Identifier"]))

# Add the identifier column
mlk_species_df = mlk_species_df.with_columns(
    pl.col("finbif_species").map_elements(lambda x: identifier_mapping.get(x, None), return_dtype=pl.Utf8).alias("identifier")
)

# Save the result
mlk_species_df.write_csv("/data/output/mlk_species_with_finbif.csv", separator=";")

