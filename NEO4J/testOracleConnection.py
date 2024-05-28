import os
import oracledb as oracle
from dotenv import load_dotenv

def test_oracle_connection(dsn, user, password):
    connection = None
    try:
        connection = oracle.connect(dsn=dsn, user=user, password=password)
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchall()
        for record in result:
            print(record)
    except oracle.DatabaseError as e:
        print("There was a problem connecting to the database:", e)
    finally:
        if connection:
            try:
                connection.close()
            except oracle.DatabaseError as e:
                print("There was a problem closing the database connection:", e)

if __name__ == "__main__":
    # Load the environment variables from dados.env using a raw string
    load_dotenv(r'C:\Users\anari\Desktop\nosql-database-evaluation\NEO4J\dados.env')
    
    # Debugging: Print environment variables to ensure they are loaded
    print(f"ORACLE_DSN: {os.getenv('ORACLE_DSN')}")
    print(f"ORACLE_USER: {os.getenv('ORACLE_USER')}")
    print(f"ORACLE_PASSWORD: {os.getenv('ORACLE_PASSWORD')}")
    
    dsn = os.getenv('ORACLE_DSN')
    user = os.getenv('ORACLE_USER')
    password = os.getenv('ORACLE_PASSWORD')
    
    if not dsn or not user or not password:
        print("One or more environment variables are missing.")
    else:
        test_oracle_connection(dsn, user, password)
