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

def check_user_permissions():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query to check user permissions
        cursor.execute("SHOW GRANTS FOR 'testuser'@'%';")
        grants = cursor.fetchall()

        print("User Permissions:")
        for grant in grants:
            print(grant[0])

        cursor.close()
        conn.close()
    except Error as e:
        print(f"Error checking permissions: {e}")

if __name__ == "__main__":
    check_user_permissions() 