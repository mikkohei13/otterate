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

try:
    print("Testing database connection...")
    print(f"Host: {DB_CONFIG['host']}")
    print(f"User: {DB_CONFIG['user']}")
    print(f"Database: {DB_CONFIG['database']}")
    
    conn = mysql.connector.connect(**DB_CONFIG)
    if conn.is_connected():
        db_info = conn.get_server_info()
        print(f"Connected to MySQL Server version {db_info}")
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print(f"Connected to database: {record[0]}")
        
        # Test if we can query the production_recordings table
        cursor.execute("SHOW TABLES LIKE 'production_recordings';")
        if cursor.fetchone():
            print("production_recordings table exists")
        else:
            print("production_recordings table does not exist")
            
        # Check user privileges
        print("\nChecking user privileges:")
        cursor.execute("SHOW GRANTS FOR CURRENT_USER;")
        grants = cursor.fetchall()
        for grant in grants:
            print(grant[0])
        
        # Check local_infile variable
        print("\nChecking local_infile system variable:")
        cursor.execute("SHOW VARIABLES LIKE 'local_infile';")
        local_infile = cursor.fetchone()
        print(f"local_infile: {local_infile[1]}")
        
except Error as e:
    print(f"Error while connecting to MySQL: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("\nMySQL connection is closed") 