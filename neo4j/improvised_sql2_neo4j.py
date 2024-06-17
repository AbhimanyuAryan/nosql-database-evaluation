import cx_Oracle
from py2neo import Graph
from datetime import datetime

def fetch_all_values_from_table(table_name, host, port, service_name, user, password):
    connection = None
    cursor = None
    try:
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        return rows
    except cx_Oracle.DatabaseError as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def parse_date(date_value, format):
    if date_value is None:
        return None
    if isinstance(date_value, datetime):
        return date_value.isoformat()
    if isinstance(date_value, str):
        return datetime.strptime(date_value, format).isoformat()
    return date_value  # If the value is not a string or datetime, return it as is

def import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, create_node_query, date_fields=None, date_format='%d-%m-%y'):
    graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))
    for row in data:
        parameters = {f"col{i}": value for i, value in enumerate(row)}
        if date_fields:
            for field in date_fields:
                if field in parameters:
                    parameters[field] = parse_date(parameters[field], date_format)
        try:
            graph.run(create_node_query, parameters=parameters)
            print(f"Imported data: {row}")
        except Exception as e:
            print(f"Failed to import data: {row} with error: {e}")

# Oracle and Neo4j connection details
oracle_host = "localhost"
oracle_port = 1521
oracle_service_name = "ORCLCDB"
oracle_user = "c##testuser"
oracle_password = "12345"
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
                DATE_SEPARATION: $col4,
                EMAIL: $col5,
                ADDRESS: $col6,
                SSN: toInteger($col7),
                IS_ACTIVE_STATUS: $col8,
                IDDEPARTMENT: $col9,
                ROLE: $col10
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
                DEDUCTIBLE: toFloat($col3),
                COVERAGE_DETAILS: $col4,
                HOSPITAL_COVERAGE: $col5,
                DENTAL_COVERAGE: $col6,
                VISION_COVERAGE: $col7
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (i:Insurance) REQUIRE i.POLICY_NUMBER IS UNIQUE;"
    },
    "MEDICAL_HISTORY": {
        "query": """
            CREATE (:Medical_History {
                RECORD_ID: toInteger($col0),
                IDPATIENT: toInteger($col1),
                DIAGNOSIS: $col2,
                RECORD_DATE: datetime($col3),
                TREATMENT: $col4
            })
        """,
        "constraints": "CREATE CONSTRAINT FOR (mh:Medical_History) REQUIRE mh.RECORD_ID IS UNIQUE;",
        "date_fields": ["col3"],
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
        import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, info["query"], date_fields=info.get("date_fields"), date_format=info.get("date_format"))

# Define relationships
relationships = [
    {
        "query": """
            MATCH (s:Staff), (d:Department)
            WHERE s.IDDEPARTMENT = d.IDDEPARTMENT
            CREATE (s)-[:WORKS_IN]->(d)
        """
    },
    {
        "query": """
            MATCH (h:Hospitalization), (r:Room)
            WHERE h.ROOM_IDROOM = r.IDROOM
            CREATE (h)-[:OCCUPIES]->(r)
        """
    },
    {
        "query": """
            MATCH (e:Episode), (p:Patient)
            WHERE e.PATIENT_IDPATIENT = p.IDPATIENT
            CREATE (e)-[:ASSOCIATED_WITH]->(p)
        """
    },
    {
        "query": """
            MATCH (b:Bill), (e:Episode)
            WHERE b.IDEPISODE = e.IDEPISODE
            CREATE (b)-[:BILLED_FOR]->(e)
        """
    },
    {
        "query": """
            MATCH (p:Prescription), (e:Episode)
            WHERE p.IDEPISODE = e.IDEPISODE
            CREATE (p)-[:PRESCRIBED_FOR]->(e)
        """
    },
    {
        "query": """
            MATCH (a:Appointment), (e:Episode)
            WHERE a.IDEPISODE = e.IDEPISODE
            CREATE (a)-[:SCHEDULED_FOR]->(e)
        """
    },
    {
        "query": """
            MATCH (ls:Lab_Screening), (e:Episode)
            WHERE ls.EPISODE_IDEPISODE = e.IDEPISODE
            CREATE (ls)-[:SCREENED_FOR]->(e)
        """
    },
    {
        "query": """
            MATCH (p:Patient), (ec:Emergency_Contact)
            WHERE p.IDPATIENT = ec.IDPATIENT
            CREATE (ec)-[:EMERGENCY_CONTACT_FOR]->(p)
        """
    },
    {
        "query": """
            MATCH (p:Patient), (i:Insurance)
            WHERE p.POLICY_NUMBER = i.POLICY_NUMBER
            CREATE (p)-[:INSURED_BY]->(i)
        """
    },
    {
        "query": """
            MATCH (mh:Medical_History), (p:Patient)
            WHERE mh.IDPATIENT = p.IDPATIENT
            CREATE (mh)-[:MEDICAL_HISTORY_OF]->(p)
        """
    }
]

# Create relationships
for relationship in relationships:
    graph.run(relationship["query"])
    print(f"Created relationship: {relationship['query']}")
