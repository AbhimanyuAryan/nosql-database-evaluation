import os
import oracledb as oracle
from dotenv import load_dotenv
from neo4j import GraphDatabase
import re
import json
from tqdm import tqdm

class Neo4jController():
    def __init__(self):
        self.OracleConnection = self.connect(option="Oracle")
        self.Neo4jConnection = self.connect(option="Neo4j")  
        if self.OracleConnection is not None and self.Neo4jConnection is not None:
            self.ensureDBs()
            self.migrate()
            self.OracleConnection.close()
            self.Neo4jConnection.close()
            print("Migration Completed")
        else:
            print("Migration Failed")

    def ensureDBs(self):
        try:
            self.ensureOracle()
            self.ensureNeo4j()
            return True
        except Exception as e:
            print("Error creating the databases")
            print("Exception: ", e)
            return False

    def migrate(self):
        try:
            cursor = self.OracleConnection.cursor()
            cursor.execute("SELECT * FROM PATIENT")
            patients = cursor.fetchall()

            for patient in tqdm(patients, desc="Migrating patients", unit="patient"):
                try:
                    patientID = patient[0]
                    patientFname = patient[1]
                    patientLname = patient[2]
                    patientBloodType = patient[3]
                    patientPhone = patient[4]
                    patientEmail = patient[5]
                    patientGender = patient[6]
                    patientPolicyNumber = patient[7]
                    patientBirthday = patient[8]

                    self.create_patient_node(patientID, patientFname, patientLname, patientBloodType, patientPhone, patientEmail, patientGender, patientPolicyNumber, patientBirthday)

                    cursor.execute(f"SELECT * FROM MEDICAL_HISTORY WHERE IDPATIENT = {patientID}")
                    medical_histories = cursor.fetchall()
                    for medical_history in medical_histories:
                        self.create_medical_history_relationship(patientID, medical_history)

                    cursor.execute(f"SELECT * FROM INSURANCE WHERE policy_number='{patientPolicyNumber}'")
                    insurance = cursor.fetchone()
                    if insurance:
                        self.create_insurance_node(insurance)
                        self.create_patient_insurance_relationship(patientID, insurance[0])

                    cursor.execute(f"SELECT * FROM EMERGENCY_CONTACT WHERE IDPATIENT = {patientID}")
                    emergency_contacts = cursor.fetchall()
                    for emergency_contact in emergency_contacts:
                        self.create_emergency_contact_relationship(patientID, emergency_contact)

                    cursor.execute(f"SELECT * FROM EPISODE WHERE PATIENT_IDPATIENT = {patientID}")
                    episodes = cursor.fetchall()
                    for episode in episodes:
                        self.create_episode_node(episode)
                        self.create_patient_episode_relationship(patientID, episode[0])

                        cursor.execute(f"SELECT * FROM PRESCRIPTION WHERE IDEPISODE = {episode[0]}")
                        prescriptions = cursor.fetchall()
                        for prescription in prescriptions:
                            self.create_prescription_relationship(episode[0], prescription)

                        cursor.execute(f"SELECT * FROM BILL WHERE IDEPISODE = {episode[0]}")
                        bills = cursor.fetchall()
                        for bill in bills:
                            self.create_bill_relationship(episode[0], bill)

                        cursor.execute(f"SELECT * FROM LAB_SCREENING WHERE EPISODE_IDEPISODE = {episode[0]}")
                        screenings = cursor.fetchall()
                        for screening in screenings:
                            self.create_lab_screening_relationship(episode[0], screening)

                        cursor.execute(f"SELECT * FROM APPOINTMENT WHERE IDEPISODE = {episode[0]}")
                        appointments = cursor.fetchall()
                        for appointment in appointments:
                            self.create_appointment_relationship(episode[0], appointment)

                        cursor.execute(f"SELECT * FROM HOSPITALIZATION WHERE IDEPISODE = {episode[0]}")
                        hospitalizations = cursor.fetchall()
                        for hospitalization in hospitalizations:
                            self.create_hospitalization_relationship(episode[0], hospitalization)

                except Exception as e:
                    print("Error migrating patient")
                    print("Exception: ", e)

        except Exception as e:
            print("Error migrating from Oracle to Neo4j")
            print("Exception: ", e)

    def create_patient_node(self, patientID, fname, lname, bloodType, phone, email, gender, policyNumber, birthday):
        query = """
        CREATE (p:Patient {id: $patientID, firstName: $fname, lastName: $lname, bloodType: $bloodType, phone: $phone, email: $email, gender: $gender, policyNumber: $policyNumber, birthday: $birthday})
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, patientID=patientID, fname=fname, lname=lname, bloodType=bloodType, phone=phone, email=email, gender=gender, policyNumber=policyNumber, birthday=birthday)

    def create_medical_history_relationship(self, patientID, medical_history):
        query = """
        MATCH (p:Patient {id: $patientID})
        CREATE (m:MedicalHistory {id: $id, condition: $condition, diagnosisDate: $diagnosisDate, treatment: $treatment})
        CREATE (p)-[:HAS_MEDICAL_HISTORY]->(m)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, patientID=patientID, id=medical_history[0], condition=medical_history[1], diagnosisDate=medical_history[2], treatment=medical_history[3])

    def create_insurance_node(self, insurance):
        query = """
        CREATE (i:Insurance {policyNumber: $policyNumber, provider: $provider, type: $type, coPay: $coPay, coverage: $coverage, maternity: $maternity, dental: $dental, vision: $vision})
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, policyNumber=insurance[0], provider=insurance[1], type=insurance[2], coPay=insurance[3], coverage=insurance[4], maternity=insurance[5], dental=insurance[6], vision=insurance[7])

    def create_patient_insurance_relationship(self, patientID, policyNumber):
        query = """
        MATCH (p:Patient {id: $patientID}), (i:Insurance {policyNumber: $policyNumber})
        CREATE (p)-[:HAS_INSURANCE]->(i)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, patientID=patientID, policyNumber=policyNumber)

    def create_emergency_contact_relationship(self, patientID, emergency_contact):
        query = """
        MATCH (p:Patient {id: $patientID})
        CREATE (e:EmergencyContact {name: $name, phone: $phone, relation: $relation})
        CREATE (p)-[:HAS_EMERGENCY_CONTACT]->(e)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, patientID=patientID, name=emergency_contact[0], phone=emergency_contact[1], relation=emergency_contact[2])

    def create_episode_node(self, episode):
        query = """
        CREATE (e:Episode {id: $id, patientID: $patientID})
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, id=episode[0], patientID=episode[1])

    def create_patient_episode_relationship(self, patientID, episodeID):
        query = """
        MATCH (p:Patient {id: $patientID}), (e:Episode {id: $episodeID})
        CREATE (p)-[:HAS_EPISODE]->(e)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, patientID=patientID, episodeID=episodeID)

    def create_prescription_relationship(self, episodeID, prescription):
        query = """
        MATCH (e:Episode {id: $episodeID})
        CREATE (p:Prescription {id: $id, date: $date, medicineID: $medicineID, dosage: $dosage})
        CREATE (e)-[:HAS_PRESCRIPTION]->(p)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, episodeID=episodeID, id=prescription[0], date=prescription[1], medicineID=prescription[3], dosage=prescription[2])

    def create_bill_relationship(self, episodeID, bill):
        query = """
        MATCH (e:Episode {id: $episodeID})
        CREATE (b:Bill {id: $id, roomCost: $roomCost, testCost: $testCost, addCharges: $addCharges, totalCost: $totalCost, registerDate: $registerDate, paymentStatus: $paymentStatus})
        CREATE (e)-[:HAS_BILL]->(b)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, episodeID=episodeID, id=bill[0], roomCost=bill[1], testCost=bill[2], addCharges=bill[3], totalCost=bill[4], registerDate=bill[6], paymentStatus=bill[7])

    def create_lab_screening_relationship(self, episodeID, screening):
        query = """
        MATCH (e:Episode {id: $episodeID})
        CREATE (s:LabScreening {id: $id, cost: $cost, date: $date, technicianID
                technicianID: $technicianID, name: $name})
        CREATE (e)-[:HAS_LAB_SCREENING]->(s)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, episodeID=episodeID, id=screening[0], cost=screening[1], date=screening[2], technicianID=screening[3], name=screening[4])

    def create_appointment_relationship(self, episodeID, appointment):
        query = """
        MATCH (e:Episode {id: $episodeID})
        CREATE (a:Appointment {id: $id, date: $date, doctorID: $doctorID})
        CREATE (e)-[:HAS_APPOINTMENT]->(a)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, episodeID=episodeID, id=appointment[0], date=appointment[1], doctorID=appointment[2])

    def create_hospitalization_relationship(self, episodeID, hospitalization):
        query = """
        MATCH (e:Episode {id: $episodeID})
        CREATE (h:Hospitalization {id: $id, admissionDate: $admissionDate, dischargeDate: $dischargeDate, roomNumber: $roomNumber})
        CREATE (e)-[:HAS_HOSPITALIZATION]->(h)
        """
        with self.Neo4jConnection.session() as session:
            session.run(query, episodeID=episodeID, id=hospitalization[0], admissionDate=hospitalization[1], dischargeDate=hospitalization[2], roomNumber=hospitalization[3])


    def connect(self, option):
        try:
            # Load environment variables from the dados.env file
            load_dotenv(r'C:\Users\anari\Desktop\nosql-database-evaluation\NEO4J\dados.env')
        except Exception as e:
            print("Error loading environment variables")
            print("Exception: ", e)
            return None

        if option == "Oracle":
            try:
                dsn = os.getenv('ORACLE_DSN')
                user = os.getenv('ORACLE_USER')
                password = os.getenv('ORACLE_PASSWORD')
                print(f"ORACLE_DSN: {dsn}")
                print(f"ORACLE_USER: {user}")
                print(f"ORACLE_PASSWORD: {password}")
                connection = oracle.connect(dsn=dsn, user=user, password=password)
                return connection
            except Exception as e:
                print("Error connecting to Oracle Database")
                print("Exception: ", e)
                return None
        elif option == "Neo4j":
            try:
                uri = os.getenv('NEO4J_URI')
                user = os.getenv('NEO4J_USER')
                password = os.getenv('NEO4J_PASSWORD')
                print(f"NEO4J_URI: {uri}")
                print(f"NEO4J_USER: {user}")
                print(f"NEO4J_PASSWORD: {password}")
                connection = GraphDatabase.driver(uri, auth=(user, password))
                return connection
            except Exception as e:
                print("Error connecting to Neo4j Database")
                print("Exception: ", e)
                return None



    def ensureOracle(self):
        cursor = self.OracleConnection.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PATIENT (
            IDPATIENT INT PRIMARY KEY,
            FNAME VARCHAR(50),
            LNAME VARCHAR(50),
            BLOOD_TYPE VARCHAR(10),
            PHONE VARCHAR(15),
            EMAIL VARCHAR(50),
            GENDER VARCHAR(10),
            POLICY_NUMBER VARCHAR(20),
            BIRTHDAY DATE
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS MEDICAL_HISTORY (
            IDHISTORY INT PRIMARY KEY,
            CONDITION VARCHAR(100),
            DIAGNOSIS_DATE DATE,
            TREATMENT VARCHAR(100),
            IDPATIENT INT,
            FOREIGN KEY (IDPATIENT) REFERENCES PATIENT(IDPATIENT)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS INSURANCE (
            POLICY_NUMBER VARCHAR(20) PRIMARY KEY,
            PROVIDER VARCHAR(50),
            TYPE VARCHAR(50),
            CO_PAY DECIMAL(10, 2),
            COVERAGE VARCHAR(100),
            MATERNITY VARCHAR(10),
            DENTAL VARCHAR(10),
            VISION VARCHAR(10)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS EMERGENCY_CONTACT (
            NAME VARCHAR(50),
            PHONE VARCHAR(15),
            RELATION VARCHAR(20),
            IDPATIENT INT,
            FOREIGN KEY (IDPATIENT) REFERENCES PATIENT(IDPATIENT)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS EPISODE (
            IDEPISODE INT PRIMARY KEY,
            PATIENT_IDPATIENT INT,
            FOREIGN KEY (PATIENT_IDPATIENT) REFERENCES PATIENT(IDPATIENT)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PRESCRIPTION (
            IDPRESCRIPTION INT PRIMARY KEY,
            DATE DATE,
            DOSAGE VARCHAR(50),
            MEDICINE_ID VARCHAR(50),
            IDEPISODE INT,
            FOREIGN KEY (IDEPISODE) REFERENCES EPISODE(IDEPISODE)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS BILL (
            IDBILL INT PRIMARY KEY,
            ROOM_COST DECIMAL(10, 2),
            TEST_COST DECIMAL(10, 2),
            ADDITIONAL_CHARGES DECIMAL(10, 2),
            TOTAL_COST DECIMAL(10, 2),
            REGISTER_DATE DATE,
            PAYMENT_STATUS VARCHAR(20),
            IDEPISODE INT,
            FOREIGN KEY (IDEPISODE) REFERENCES EPISODE(IDEPISODE)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS LAB_SCREENING (
            IDSCREENING INT PRIMARY KEY,
            COST DECIMAL(10, 2),
            DATE DATE,
            TECHNICIAN_ID VARCHAR(50),
            NAME VARCHAR(50),
            EPISODE_IDEPISODE INT,
            FOREIGN KEY (EPISODE_IDEPISODE) REFERENCES EPISODE(IDEPISODE)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS APPOINTMENT (
            IDAPPOINTMENT INT PRIMARY KEY,
            DATE DATE,
            DOCTOR_ID VARCHAR(50),
            IDEPISODE INT,
            FOREIGN KEY (IDEPISODE) REFERENCES EPISODE(IDEPISODE)
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS HOSPITALIZATION (
            IDHOSPITALIZATION INT PRIMARY KEY,
            ADMISSION_DATE DATE,
            DISCHARGE_DATE DATE,
            ROOM_NUMBER VARCHAR(10),
            IDEPISODE INT,
            FOREIGN KEY (IDEPISODE) REFERENCES EPISODE(IDEPISODE)
        )
        """)
        self.OracleConnection.commit()

    def ensureNeo4j(self):
        with self.Neo4jConnection.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (p:Patient) ASSERT p.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (i:Insurance) ASSERT i.policyNumber IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (e:Episode) ASSERT e.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (m:MedicalHistory) ASSERT m.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (p:Prescription) ASSERT p.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (b:Bill) ASSERT b.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (s:LabScreening) ASSERT s.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (a:Appointment) ASSERT a.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS ON (h:Hospitalization) ASSERT h.id IS UNIQUE")

if __name__ == "__main__":
    Neo4jController()

