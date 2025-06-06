# This script uploads observations to a MariaDB database.
# Observations are creaated by joining species identifications from a csv file and joining recording data to them from a MariaDB database table

import csv
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create a database connection"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return connection
    except Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None

def process_recordings():
    """Process recordings from CSV and join with database data"""
    connection = get_db_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor(dictionary=True)
        processed_count = 0
        
        # Read CSV file
        with open('/data/species_ids_sample.csv', 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            
            for row in csv_reader:
                if processed_count >= 10:
                    break
                    
                # Get recording data from database
                rec_id = row['rec_id']
                cursor.execute("""
                    SELECT * FROM production_recordings 
                    WHERE id = %s
                """, (rec_id,))
                
                recording = cursor.fetchone()
                
                if recording:
                    # Print joined data for debugging
                    print("\nProcessed recording:")
                    print(f"CSV data: {row}")
                    print(f"Database data: {recording}")
                    print("-" * 50)
                
                processed_count += 1
                
    except Error as e:
        print(f"Error processing data: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    process_recordings()

