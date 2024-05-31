import cx_Oracle

def fetch_all_values_from_appointment_table(host, port, service_name, user, password):
    connection = None
    cursor = None
    try:
        # Create the Oracle connection string
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        
        # Establish the connection
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()
        
        # Execute the query to get all values from the BILL table
        cursor.execute("SELECT * FROM APPOINTMENT")
        
        # Fetch and print all rows
        rows = cursor.fetchall()
        if rows:
            print("Values in the APPOINTMENT table:")
            for row in rows:
                print(row)
        else:
            print("No values found in the APPOINTMENT table.")

    except cx_Oracle.DatabaseError as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up and close the connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Replace the following with your Oracle DB details
host = "localhost"
port = 1521
service_name = "ORCLCDB"  # Replace with your service name if different
user = "c##testuser"
password = "12345"

# Call the function to fetch all values from the BILL table
fetch_all_values_from_appointment_table(host, port, service_name, user, password)