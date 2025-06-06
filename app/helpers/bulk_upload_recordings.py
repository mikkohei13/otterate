'''
For unknown reasons, the bulk upload gives this error:

root@0a215651bf5f:/app# cd /app/app && python3 bulk_upload_recordings.py
Connecting to the database...
Disabling indexes temporarily...
Starting bulk data upload...
Error uploading data: 1045 (28000): Access denied for user 'testuser'@'%' (using password: YES)
'''

import mysql.connector
from mysql.connector import Error
import os

# Database connection details from environment variables
DB_CONFIG = {
    'host': os.getenv('MARIADB_HOST'),
    'user': os.getenv('MARIADB_USER'),
    'password': os.getenv('MARIADB_PASSWORD'),
    'database': os.getenv('MARIADB_DATABASE')
}

def bulk_upload_data(csv_file_path):
    try:
        print("Connection details:")
        print(f"Host: {DB_CONFIG['host']}")
        print(f"User: {DB_CONFIG['user']}")
        print(f"Database: {DB_CONFIG['database']}")
        print("Connecting to the database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("Disabling indexes temporarily...")
        cursor.execute("ALTER TABLE production_recordings DISABLE KEYS;")

        print("Starting bulk data upload...")
        load_data_query = f"""
        LOAD DATA INFILE '{csv_file_path}'
        IGNORE
        INTO TABLE production_recordings
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (user_anon, date, time, @len, dur, @real_obs, rec_type, point_count_loc, lat, lon, url, rec_id)
        SET len = IF(@len = '', 0.0, @len),
            real_obs = IF(@real_obs = 'true', 1, 0)
        """
        cursor.execute(load_data_query)

        print("Re-enabling indexes...")
        cursor.execute("ALTER TABLE production_recordings ENABLE KEYS;")

        conn.commit()
        cursor.close()
        conn.close()
        print("Data uploaded successfully using bulk insert.")
    except Error as e:
        print(f"Error uploading data: {e}")

if __name__ == "__main__":
    bulk_upload_data('/data/recordings_anon.csv') 