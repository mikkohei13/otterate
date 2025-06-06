# Script to test reading data from csv datafiles and inserting into mariadb

import csv
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

# Function to create database and table if they don't exist
def setup_database():
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS production_recordings (
                user_anon VARCHAR(255),
                date DATE,
                time TIME,
                len FLOAT,
                dur FLOAT,
                real_obs BOOLEAN,
                rec_type VARCHAR(255),
                point_count_loc VARCHAR(255),
                lat FLOAT,
                lon FLOAT,
                url TEXT,
                rec_id VARCHAR(255) PRIMARY KEY
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Database and table setup completed.")
    except Error as e:
        print(f"Error setting up database: {e}")

# Function to upload data from CSV to MariaDB
def upload_data(limit=10):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        skipped_count = 0
        entered_count = 0
        with open('/data/recordings_anon.csv', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Skip header
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                # Print status every n rows
                if i % 1000 == 0:
                    print(f"Processing row {i}")
                # Convert real_obs to boolean
                real_obs = row[5].lower() == 'true'
                # Replace empty 'len' value with 0.0
                len_value = 0.0 if row[3] == '' else float(row[3])
                try:
                    cursor.execute("""
                        INSERT IGNORE INTO production_recordings (user_anon, date, time, len, dur, real_obs, rec_type, point_count_loc, lat, lon, url, rec_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (row[0], row[1], row[2], len_value, row[4], real_obs, row[6], row[7], row[8], row[9], row[10], row[11]))
                    if cursor.rowcount == 0:
                        skipped_count += 1
                        if skipped_count % 1000 == 0:
                            print(f"Skipped {skipped_count} rows due to duplicate rec_id.")
                    else:
                        entered_count += 1
                except Error as e:
                    print(f"Error inserting row: {e}")
                    continue
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Data uploaded successfully. Entered: {entered_count}, Skipped: {skipped_count}.")
    except Error as e:
        print(f"Error uploading data: {e}")

# Main execution
if __name__ == "__main__":
    setup_database()
    upload_data(limit=10000000)

