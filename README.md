# Otterate

Otterate is a data processing and analysis toolkit designed for cleaning, analyzing, and preparing bird observation data for ingestion into the FinBIF (Finnish Biodiversity Information Facility) Data Bank. The project focuses on processing AI-identified bird recordings and comparing them against existing bird atlas data to identify new or interesting observations.

**⚠️ Note: This project is still a work in progress.**

## Features

- **Data Analysis**: Process large Parquet files containing bird observation data
- **Atlas Comparison**: Compare observations against existing bird atlas data to find new species in specific grid squares
- **Data Cleaning**: Filter and validate observation data based on prediction confidence, geographic boundaries, and other criteria
- **Database Integration**: Upload processed data to MariaDB databases
- **Geographic Processing**: Work with Finnish uniform grid system (YKJ) coordinates
- **Caching**: Cache API responses to avoid repeated requests to external services

## Prerequisites

- Python 3.9+
- Docker and Docker Compose
- Access to FinBIF API (requires API token)
- MariaDB/MySQL database (optional, for data uploads)

## Installation With Docker

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd otterate
   ```

2. Create a `.env` file with your configuration:
   ```bash
   # FinBIF API configuration
   FINBIF_API_TOKEN=your_api_token_here
   
   # Database configuration (if using database features)
   MARIADB_HOST=localhost
   MARIADB_USER=your_username
   MARIADB_PASSWORD=your_password
   MARIADB_DATABASE=your_database
   ```

3. Build and run with Docker Compose:
   ```bash
   docker-compose up --build
   ```

## Usage

### Data Paths

The system expects data files in specific locations:
- Observation data: `/data/observations.parquet`
- Atlas squares: `./data/atlas_squares.csv`
- Output directory: `./app/output/`
- Cache directory: `./app/cache/`

### Data preprocessing

* app/prepare_recording_data.py: Upload processed data to a Parquet file.
* app/prepare_ykj.py: Add YKJ coordinates to a Parquet file.

### app/analyze_data.py

Generates statistics of the observation data:

- Loads observation data from Parquet files
- Filters data by prediction confidence (>0.9)
- Restricts observations to Finnish geographic boundaries
- Generates species count statistics
- Saves results to output files

### app/atlas.py

Compares observations against bird atlas data:

- Processes atlas grid squares
- Fetches existing species data from the atlas API
- Identifies observations of species not yet recorded in specific squares
- Saves interesting observations for further review

## Data Format

The system expects bird observation data in the following format:

- **Input**: Parquet files with columns including:
  - `user_anon`: Anonymous user identifier
  - `date`, `time`: Recording timestamp
  - `lat`, `lon`: Geographic coordinates
  - `species`: Bird species identification
  - `prediction`: AI confidence score (0-1)
  - `n`, `e`: YKJ grid coordinates
  - `rec_id`: Unique recording identifier

- **Output**: CSV files with filtered and processed observations

