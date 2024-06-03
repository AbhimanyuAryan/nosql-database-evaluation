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
                IDDEPARTMENT: $col9
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (s:Staff) REQUIRE s.EMP_ID IS UNIQUE;"
    },
    "DOCTOR": {
        "query": """
            CREATE (:Doctor {
                EMP_ID: toInteger($col0),
                QUALIFICATIONS: $col1
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (d:Doctor) REQUIRE d.EMP_ID IS UNIQUE;"
    },
    "TECHNICIAN": {
        "query": """
            CREATE (:Technician {
                STAFF_EMP_ID: toInteger($col0)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (t:Technician) REQUIRE t.STAFF_EMP_ID IS UNIQUE;"
    },
    "NURSE": {
        "query": """
            CREATE (:Nurse {
                STAFF_EMP_ID: toInteger($col0)
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (n:Nurse) REQUIRE n.STAFF_EMP_ID IS UNIQUE;"
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

# Connect to Neo4j and create constraints
graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))
for table_name, info in tables_and_queries.items():
    constraint_query = info.get('constraints')
    if constraint_query:
        graph.run(constraint_query)
        print(f"Created constraint for {table_name}")

# Fetch data from Oracle and import to Neo4j
for table_name, info in tables_and_queries.items():
    data = fetch_all_values_from_table(table_name, oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)
    if data:
        date_fields = info.get('date_fields')
        date_format = info.get('date_format', '%d-%m-%y')
        import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, info['query'], date_fields, date_format)
    else:
        print(f"No data to import for table: {table_name}")

# Create relationships between nodes
relationships = [
    """
    MATCH (staff:Staff), (dept:Department)
    WHERE staff.IDDEPARTMENT = dept.IDDEPARTMENT
    MERGE (staff)-[:WORKS_IN]->(dept)
    """,
    """
    MATCH (doc:Doctor), (staff:Staff)
    WHERE doc.EMP_ID = staff.EMP_ID
    MERGE (doc)-[:IS_DOCTOR_FOR]->(staff)
    """,
    """
    MATCH (tech:Technician), (staff:Staff)
    WHERE tech.STAFF_EMP_ID = staff.EMP_ID
    MERGE (tech)-[:IS_TECHNICIAN_FOR]->(staff)
    """,
    """
    MATCH (nur:Nurse), (staff:Staff)
    WHERE nur.STAFF_EMP_ID = staff.EMP_ID
    MERGE (nur)-[:IS_NURSE_FOR]->(staff)
    """,
    """
    MATCH (h:Hospitalization), (n:Nurse)
    WHERE h.RESPONSIBLE_NURSE = n.STAFF_EMP_ID
    MERGE (h)-[:RESPONSIBLE_FOR]->(n)
    """,
    """
    MATCH (h:Hospitalization), (r:Room)
    WHERE h.ROOM_IDROOM = r.IDROOM
    MERGE (h)-[:ASSIGNED_TO]->(r)
    """,
    """
    MATCH (h:Hospitalization), (e:Episode)
    WHERE h.IDEPISODE = e.IDEPISODE
    MERGE (h)-[:INVOLVES]->(e)
    """,
    """
    MATCH (b:Bill), (e:Episode)
    WHERE b.IDEPISODE = e.IDEPISODE
    MERGE (b)-[:BILLED_FOR]->(e)
    """,
    """
    MATCH (p:Prescription), (e:Episode)
    WHERE p.IDEPISODE = e.IDEPISODE
    MERGE (p)-[:PRESCRIBED_FOR]->(e)
    """,
    """
    MATCH (p:Prescription), (m:Medicine)
    WHERE p.IDMEDICINE = m.IDMEDICINE
    MERGE (p)-[:PRESCRIBED_MEDICINE]->(m)
    """,
    """
    MATCH (a:Appointment), (d:Doctor)
    WHERE a.IDDOCTOR = d.EMP_ID
    MERGE (a)-[:HAS_DOCTOR]->(d)
    """,
    """
    MATCH (a:Appointment), (e:Episode)
    WHERE a.IDEPISODE = e.IDEPISODE
    MERGE (a)-[:BELONGS_TO_EPISODE]->(e)
    """,
    """
    MATCH (ls:Lab_Screening), (ep:Episode)
    WHERE ls.EPISODE_IDEPISODE = ep.IDEPISODE
    MERGE (ls)-[:BELONGS_TO]->(ep)
    """,
    """
    MATCH (ls:Lab_Screening), (tech:Technician)
    WHERE ls.IDTECHNICIAN = tech.STAFF_EMP_ID
    MERGE (ls)-[:PERFORMED_BY]->(tech)
    """,
    """
    MATCH (p:Patient), (e:Episode)
    WHERE p.IDPATIENT = e.PATIENT_IDPATIENT
    MERGE (p)-[:HAS_EPISODE]->(e)
    """,
    """
    MATCH (p:Patient), (ec:Emergency_Contact)
    WHERE p.IDPATIENT = ec.IDPATIENT
    MERGE (ec)-[:CONTACT_FOR]->(p)
    """,
    """
    MATCH (p:Patient), (i:Insurance)
    WHERE p.POLICY_NUMBER = i.POLICY_NUMBER
    MERGE (p)-[:HAS_INSURANCE]->(i)
    """,
    """
    MATCH (p:Patient), (mh:Medical_History)
    WHERE p.IDPATIENT = mh.IDPATIENT
    MERGE (p)-[:HAS_MEDICAL_HISTORY]->(mh)
    """
]

# Execute relationship queries in Neo4j
for query in relationships:
    graph.run(query)
    print(f"Executed relationship query: {query}")

print("Data import and relationship creation completed.")
