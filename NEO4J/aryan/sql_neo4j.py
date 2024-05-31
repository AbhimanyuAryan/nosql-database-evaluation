import cx_Oracle
from py2neo import Graph, Node

def fetch_all_values_from_appointment_table(host, port, service_name, user, password):
    connection = None
    cursor = None
    try:
        # Create the Oracle connection string
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        
        # Establish the connection
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()
        
        # Execute the query to get all values from the APPOINTMENT table
        cursor.execute("SELECT * FROM APPOINTMENT")
        
        # Fetch and return all rows
        rows = cursor.fetchall()
        return rows

    except cx_Oracle.DatabaseError as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        # Clean up and close the connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def import_appointments_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, appointments):
    # Connect to Neo4j
    graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))

    for appointment in appointments:
        # Create a Neo4j node for each appointment
        node = Node("Appointment", id=appointment[0], date=appointment[1], patient_id=appointment[2], doctor_id=appointment[3])
        graph.create(node)
        print(f"Imported appointment: {appointment}")

# Oracle DB details
oracle_host = "localhost"
oracle_port = 1521
oracle_service_name = "ORCLCDB"
oracle_user = "c##testuser"
oracle_password = "12345"

# Fetch all values from the APPOINTMENT table
appointments = fetch_all_values_from_appointment_table(oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)

if appointments:
    # Neo4j DB details
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "12345678"
    
    # Import appointments into Neo4j
    import_appointments_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, appointments)
else:
    print("No data to import.")
