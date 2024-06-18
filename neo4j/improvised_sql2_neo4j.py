import cx_Oracle
from py2neo import Graph
from datetime import datetime

def fetch_all_values_from_table(table_name, host, port, service_name, user, password):
    connection = None
    cursor = None
    try:
        # Create the Oracle connection string
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        
        # Establish the connection
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()
        
        # Execute the query to get all values from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        
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

def parse_date(date_value, format):
    if date_value is None:
        return None
    if isinstance(date_value, datetime):
        return date_value.isoformat()
    return datetime.strptime(date_value, format).isoformat()

def import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, create_node_query, date_fields=None, date_format='%d-%m-%y'):
    # Connect to Neo4j
    graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))

    for row in data:
        parameters = {f"col{i}": value for i, value in enumerate(row)}
        
        # Handle date parsing
        if date_fields:
            for field in date_fields:
                parameters[field] = parse_date(parameters[field], date_format)
        
        # Execute the Cypher query to create a node
        graph.run(create_node_query, parameters=parameters)
        print(f"Imported data: {row}")

# Oracle DB details
oracle_host = "localhost"
oracle_port = 1521
oracle_service_name = "ORCLCDB"
oracle_user = "c##testuser"
oracle_password = "12345"

# Neo4j DB details
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "12345678"

# List of tables and their corresponding Cypher create queries
tables_and_queries = {
    "DEPARTMENT": {
        "query": """
            CREATE (:Department {
                IDDEPARTMENT: $col0,
                DEPT_HEAD: $col1,
                DEPT_NAME: $col2,
                EMP_COUNT: toInteger($col3)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (d:Department) REQUIRE d.IDDEPARTMENT IS UNIQUE;"
    },
    "STAFF": {
        "query": """
            CREATE (:Staff {
                EMP_ID: toInteger($col0),
                EMP_FNAME: $col1,
                EMP_LNAME: $col2,
                DATE_JOINING: $col3,
                DATE_SEPERATION: $col4,
                EMAIL: $col5,
                ADDRESS: $col6,
                SSN: toInteger($col7),
                IS_ACTIVE_STATUS: $col8,
                IDDEPARTMENT: $col9,
                ROLE: $col10,
                QUALIFICATIONS: $col11
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (s:Staff) REQUIRE s.EMP_ID IS UNIQUE;"
    },
    "HOSPITALIZATION": {
        "query": """
            CREATE (:Hospitalization {
                ADMISSION_DATE: datetime($col0),
                DISCHARGE_DATE: CASE WHEN $col1 IS NOT NULL THEN datetime($col1) ELSE NULL END,
                ROOM_IDROOM: toInteger($col2),
                IDEPISODE: toInteger($col3),
                RESPONSIBLE_NURSE: toInteger($col4)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (h:Hospitalization) REQUIRE h.IDEPISODE IS UNIQUE;",
        "date_fields": ["col0", "col1"],
        "date_format": "%d-%m-%y"
    },
    "ROOM": {
        "query": """
            CREATE (:Room {
                IDROOM: toInteger($col0),
                ROOM_TYPE: $col1,
                ROOM_COST: toInteger($col2)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (r:Room) REQUIRE r.IDROOM IS UNIQUE;"
    },
    "EPISODE": {
        "query": """
            CREATE (:Episode {
                IDEPISODE: toInteger($col0),
                PATIENT_IDPATIENT: toInteger($col1)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (e:Episode) REQUIRE e.IDEPISODE IS UNIQUE;"
    },
    "BILL": {
        "query": """
            CREATE (:Bill {
                IDBILL: toInteger($col0),
                ROOM_COST: toInteger($col1),
                TEST_COST: toInteger($col2),
                OTHER_CHARGES: toInteger($col3),
                TOTAL: toInteger($col4),
                IDEPISODE: toInteger($col5),
                REGISTERED_AT: datetime($col6),
                PAYMENT_STATUS: $col7
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (b:Bill) REQUIRE b.IDBILL IS UNIQUE;",
        "date_fields": ["col6"],
        "date_format": "%d-%m-%y %H:%M:%S.%f"
    },
    "PRESCRIPTION": {
        "query": """
            CREATE (:Prescription {
                IDPRESCRIPTION: toInteger($col0),
                PRESCRIPTION_DATE: datetime($col1),
                DOSAGE: toInteger($col2),
                IDMEDICINE: toInteger($col3),
                IDEPISODE: toInteger($col4)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (p:Prescription) REQUIRE p.IDPRESCRIPTION IS UNIQUE;",
        "date_fields": ["col1"],
        "date_format": "%d-%m-%y"
    },
    "MEDICINE": {
        "query": """
            CREATE (:Medicine {
                IDMEDICINE: toInteger($col0),
                M_NAME: $col1,
                M_QUANTITY: toInteger($col2),
                M_COST: toFloat($col3)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (m:Medicine) REQUIRE m.IDMEDICINE IS UNIQUE;"
    },
    "APPOINTMENT": {
        "query": """
            CREATE (:Appointment {
                SCHEDULED_ON: datetime($col0),
                APPOINTMENT_DATE: datetime($col1),
                APPOINTMENT_TIME: $col2,
                IDDOCTOR: toInteger($col3),
                IDEPISODE: toInteger($col4)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (a:Appointment) REQUIRE (a.APPOINTMENT_DATE, a.APPOINTMENT_TIME, a.IDDOCTOR, a.IDEPISODE) IS UNIQUE;",
        "date_fields": ["col0", "col1"],
        "date_format": "%d-%m-%y"
    },
    "LAB_SCREENING": {
        "query": """
            CREATE (:Lab_Screening {
                LAB_ID: toInteger($col0),
                TEST_COST: toFloat($col1),
                TEST_DATE: datetime($col2),
                IDTECHNICIAN: toInteger($col3),
                EPISODE_IDEPISODE: toInteger($col4)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (ls:Lab_Screening) REQUIRE ls.LAB_ID IS UNIQUE;",
        "date_fields": ["col2"],
        "date_format": "%d-%m-%y"
    },
    "PATIENT": {
        "query": """
            CREATE (:Patient {
                IDPATIENT: toInteger($col0),
                PATIENT_FNAME: $col1,
                PATIENT_LNAME: $col2,
                BLOOD_TYPE: $col3,
                PHONE: $col4,
                EMAIL: $col5,
                GENDER: $col6,
                POLICY_NUMBER: $col7,
                BIRTHDAY: datetime($col8)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (p:Patient) REQUIRE p.IDPATIENT IS UNIQUE;",
        "date_fields": ["col8"],
        "date_format": "%d-%m-%y"
    },
    "EMERGENCY_CONTACT": {
        "query": """
            CREATE (:Emergency_Contact {
                CONTACT_NAME: $col0,
                PHONE: $col1,
                RELATION: $col2,
                IDPATIENT: toInteger($col3)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (ec:Emergency_Contact) REQUIRE (ec.IDPATIENT, ec.PHONE) IS UNIQUE;"
    },
    "INSURANCE": {
        "query": """
            CREATE (:Insurance {
                POLICY_NUMBER: $col0,
                PROVIDER: $col1,
                INSURANCE_PLAN: $col2,
                CO_PAY: toInteger($col3),
                COVERAGE: $col4,
                MATERNITY: $col5,
                DENTAL: $col6,
                OPTICAL: $col7
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (i:Insurance) REQUIRE i.POLICY_NUMBER IS UNIQUE;"
    },
    "MEDICAL_HISTORY": {
        "query": """
            CREATE (:Medical_History {
                RECORD_ID: toInteger($col0),
                CONDITION: $col1,
                RECORD_DATE: datetime($col2),
                IDPATIENT: toInteger($col3)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (mh:Medical_History) REQUIRE mh.RECORD_ID IS UNIQUE;",
        "date_fields": ["col2"],
        "date_format": "%d-%m-%y"
    }
}

# Function to merge staff roles data
def merge_staff_data(staff_data, role_data, role, qualification_field=None):
    emp_id_index = 0  # Assuming EMP_ID is the first column in staff_data
    role_index = len(staff_data[0])  # New role column index
    qualification_index = role_index + 1  # New qualification column index

    merged_data = []
    for staff_row in staff_data:
        emp_id = staff_row[emp_id_index]
        matching_role_row = next((row for row in role_data if row[0] == emp_id), None)
        if matching_role_row:
            qualification = matching_role_row[1] if qualification_field else None
            merged_row = list(staff_row) + [role, qualification]
        else:
            merged_row = list(staff_row) + [None, None]
        merged_data.append(merged_row)
    
    return merged_data

# Connect to Neo4j and create constraints
graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))
for table_name, info in tables_and_queries.items():
    constraint_query = info.get('constraints')
    if constraint_query:
        graph.run(constraint_query)
        print(f"Created constraint for {table_name}")

# Fetch data from Oracle
staff_data = fetch_all_values_from_table("STAFF", oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)
doctor_data = fetch_all_values_from_table("DOCTOR", oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)
technician_data = fetch_all_values_from_table("TECHNICIAN", oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)
nurse_data = fetch_all_values_from_table("NURSE", oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)

# Merge role data into staff data
staff_data = merge_staff_data(staff_data, doctor_data, 'Doctor', qualification_field=True)
staff_data = merge_staff_data(staff_data, technician_data, 'Technician')
staff_data = merge_staff_data(staff_data, nurse_data, 'Nurse')

# Import merged staff data to Neo4j
import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, staff_data, tables_and_queries["STAFF"]["query"])

# Fetch and import other tables data, skipping STAFF, TECHNICIAN, DOCTOR, and NURSE
for table_name, info in tables_and_queries.items():
    if table_name in ["STAFF", "TECHNICIAN", "DOCTOR", "NURSE"]:
        continue  # Skip the tables that have been merged and processed
    
    data = fetch_all_values_from_table(table_name, oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)
    if data:
        date_fields = info.get('date_fields')
        date_format = info.get('date_format', '%d-%m-%y')
        import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, info['query'], date_fields, date_format)
    else:
        print(f"No data to import for table: {table_name}")