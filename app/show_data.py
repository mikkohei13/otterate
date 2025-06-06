# Script for showing sample data in mariadb

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

def show_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM recordings LIMIT 10")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        conn.close()
    except Error as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    show_data() 