import cx_Oracle
from py2neo import Graph

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

def import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, create_node_query):
    # Connect to Neo4j
    graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))

    for row in data:
        # Execute the Cypher query to create a node
        graph.run(create_node_query, parameters={"row": row})
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
                IDDEPARTMENT: $row[0],
                DEPT_HEAD: $row[1],
                DEPT_NAME: $row[2],
                EMP_COUNT: toInteger($row[3])
            })
        """
    },
    "STAFF": {
        "query": """
            CREATE (:Staff {
                EMP_ID: toInteger($row[0]),
                EMP_FNAME: $row[1],
                EMP_LNAME: $row[2],
                DATE_JOINING: $row[3],
                DATE_SEPERATION: $row[4],
                EMAIL: $row[5],
                ADDRESS: $row[6],
                SSN: toInteger($row[7]),
                IS_ACTIVE_STATUS: $row[8],
                IDDEPARTMENT: $row[9]
            })
        """
    },
    "DOCTOR": {
        "query": """
            CREATE (:Doctor {
                EMP_ID: toInteger($row[0]),
                QUALIFICATIONS: $row[1]
            })
        """
    },
    "TECHNICIAN": {
        "query": """
            CREATE (:Technician {
                STAFF_EMP_ID: toInteger($row[0])
            })
        """
    },
    "NURSE": {
        "query": """
            CREATE (:Nurse {
                STAFF_EMP_ID: toInteger($row[0])
            })
        """
    },
    "HOSPITALIZATION": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[0], 'ms', 'dd-MM-yy') AS admissionMillis,
                 apoc.date.parse(row[1], 'ms', 'dd-MM-yy') AS dischargeMillis
            CREATE (:Hospitalization {
                ADMISSION_DATE: date(datetime({epochMillis: admissionMillis})),
                DISCHARGE_DATE: CASE WHEN row[1] IS NOT NULL THEN date(datetime({epochMillis: dischargeMillis})) ELSE NULL END,
                ROOM_IDROOM: toInteger(row[2]),
                IDEPISODE: toInteger(row[3]),
                RESPONSIBLE_NURSE: toInteger(row[4])
            })
        """
    },
    "ROOM": {
        "query": """
            CREATE (:Room {
                IDROOM: toInteger($row[0]),
                ROOM_TYPE: $row[1],
                ROOM_COST: toInteger($row[2])
            })
        """
    },
    "EPISODE": {
        "query": """
            CREATE (:Episode {
                IDEPISODE: toInteger($row[0]),
                PATIENT_IDPATIENT: toInteger($row[1])
            })
        """
    },
    "BILL": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[6], 'ms', 'yy-MM-dd hh:mm:ss.SSSSSSSSS a') AS registeredMillis
            CREATE (:Bill {
                IDBILL: toInteger(row[0]),
                ROOM_COST: toInteger(row[1]),
                TEST_COST: toInteger(row[2]),
                OTHER_CHARGES: toInteger(row[3]),
                TOTAL: toInteger(row[4]),
                IDEPISODE: toInteger(row[5]),
                REGISTERED_AT: datetime({epochMillis: registeredMillis}),
                PAYMENT_STATUS: row[7]
            })
        """
    },
    "PRESCRIPTION": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[1], 'ms', 'dd-MM-yy') AS prescriptionMillis
            CREATE (:Prescription {
                IDPRESCRIPTION: toInteger(row[0]),
                PRESCRIPTION_DATE: date(datetime({epochMillis: prescriptionMillis})),
                DOSAGE: toInteger(row[2]),
                IDMEDICINE: toInteger(row[3]),
                IDEPISODE: toInteger(row[4])
            })
        """
    },
    "MEDICINE": {
        "query": """
            CREATE (:Medicine {
                IDMEDICINE: toInteger($row[0]),
                M_NAME: $row[1],
                M_QUANTITY: toInteger($row[2]),
                M_COST: toFloat($row[3])
            })
        """
    },
    "APPOINTMENT": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[0], 'ms', 'dd-MM-yy') AS scheduledMillis,
                 apoc.date.parse(row[1], 'ms', 'dd-MM-yy') AS appointmentMillis
            CREATE (:Appointment {
                SCHEDULED_ON: date(datetime({epochMillis: scheduledMillis})),
                APPOINTMENT_DATE: date(datetime({epochMillis: appointmentMillis})),
                APPOINTMENT_TIME: row[2],
                IDDOCTOR: toInteger(row[3]),
                IDEPISODE: toInteger(row[4])
            })
        """
    },
    "LAB_SCREENING": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[2], 'ms', 'dd-MM-yy') AS testMillis
            CREATE (:Lab_Screening {
                LAB_ID: toInteger(row[0]),
                TEST_COST: toFloat(row[1]),
                TEST_DATE: date(datetime({epochMillis: testMillis})),
                IDTECHNICIAN: toInteger(row[3]),
                EPISODE_IDEPISODE: toInteger(row[4])
            })
        """
    },
    "PATIENT": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[8], 'ms', 'dd-MM-yy') AS birthMillis
            CREATE (:Patient {
                IDPATIENT: toInteger(row[0]),
                PATIENT_FNAME: row[1],
                PATIENT_LNAME: row[2],
                BLOOD_TYPE: row[3],
                PHONE: row[4],
                EMAIL: row[5],
                GENDER: row[6],
                POLICY_NUMBER: row[7],
                BIRTHDAY: date(datetime({epochMillis: birthMillis}))
            })
        """
    },
    "EMERGENCY_CONTACT": {
        "query": """
            CREATE (:Emergency_Contact {
                CONTACT_NAME: $row[0],
                PHONE: $row[1],
                RELATION: $row[2],
                IDPATIENT: toInteger($row[3])
            })
        """
    },
    "INSURANCE": {
        "query": """
            CREATE (:Insurance {
                POLICY_NUMBER: $row[0],
                PROVIDER: $row[1],
                INSURANCE_PLAN: $row[2],
                CO_PAY: toInteger($row[3]),
                COVERAGE: $row[4],
                MATERNITY: $row[5],
                DENTAL: $row[6],
                OPTICAL: $row[7]
            })
        """
    },
    "MEDICAL_HISTORY": {
        "query": """
            WITH $row AS row,
                 apoc.date.parse(row[2], 'ms', 'dd-MM-yy') AS recordMillis
            CREATE (:Medical_History {
                RECORD_ID: toInteger(row[0]),
                CONDITION: row[1],
                RECORD_DATE: date(datetime({epochMillis: recordMillis})),
                IDPATIENT: toInteger(row[3])
            })
        """
    }
}

# Fetch data from Oracle and import to Neo4j
for table_name, info in tables_and_queries.items():
    data = fetch_all_values_from_table(table_name, oracle_host, oracle_port, oracle_service_name, oracle_user, oracle_password)
    if data:
        import_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, data, info['query'])
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
graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))
for query in relationships:
    graph.run(query)
    print(f"Executed relationship query: {query}")

print("Data import and relationship creation completed.")
