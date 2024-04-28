import os
import oracledb as oracle
from dotenv import load_dotenv
import pymongo
import re

import sys
sys.path.append('./structs/')
from structs import *

class mongoDBController():
    def __init__(self):
        #self.run_requirements()
        self.OracleConnection = self.connect(option="Oracle")
        self.MongoConnection = self.connect(option="MongoDB")  
        if self.OracleConnection!=None and self.MongoConnection!=None:
            # self.dropDBs()
            # self.ensureDBs()
            self.migrate()
            self.OracleConnection.close()
            self.MongoConnection.close()
            print("Migration Completed")
        else:
            print("Migration Failed")

    def dropDBs(self):
        #Drop the Oracle database
        try:
            cursor = self.OracleConnection.cursor()
            # List all tables
            cursor.execute("SELECT table_name FROM user_tables")
            tables = cursor.fetchall()
            for table in tables:
                cursor.execute("DROP TABLE " + table[0] + " CASCADE CONSTRAINTS")

            # List all sequences
            cursor.execute("SELECT sequence_name FROM user_sequences")
            sequences = cursor.fetchall()
            for sequence in sequences:
                cursor.execute("DROP SEQUENCE " + sequence[0])
            
            # List all views
            cursor.execute("SELECT view_name FROM user_views")
            views = cursor.fetchall()
            for view in views:
                cursor.execute("DROP VIEW " + view[0])
            
            # List all triggers
            cursor.execute("SELECT trigger_name FROM user_triggers")
            triggers = cursor.fetchall()
            for trigger in triggers:
                cursor.execute("DROP TRIGGER " + trigger[0])
            print("Oracle database dropped")
        except Exception as e:
            print("Error dropping the Oracle database")
            print("Exception: ", e)
        
        #Drop the MongoDB database
        try:
            self.MongoConnection.drop_database("BDNOSQLTP")
            print("MongoDB database dropped")
        except Exception as e:
            print("Error dropping the MongoDB database")
            print("Exception: ", e)
    


    def ensureDBs(self):
        #Verify if the database is running in the Oracle connection
        #If not, create it
        #Verify if the database is running in the MongoDB connection
        #If not, create it
        try:
            self.ensureOracle()
            # self.ensureMongo()
            return True
        except Exception as e:
            print("Error creating the databases")
            print("Exception: ", e)
            return False
        
       # Episode: Represents the episodes of a particular patient. Episodes of hospitalization and
        # consultation are considered.
        # Patient: Stores information about patients.
        # Department: Maintains details about the hospital's departments.
        # Staff: Records data of hospital staff.
        # Doctor: Stores information specific to the hospital's doctors.
        # Nurse: Contains details about the nurses at the hospital.
        # Technician: Contains details about the hospital's technicians.
        # Emergency_Contact (Emergency Contact): Records a patient's emergency contacts.
        # Lab_Screening (Lab Screening): Contains information about lab tests.
        # Insurance: Maintains details about patients' insurance.
        # Medicine: Stores information about medications.
        # Prescription: Records medical prescriptions.
        # Medical_History (Medical History): Maintains the medical history of patients.
        # Appointment: Contains details about appointments.
        # Hospitalization: Contains details about hospitalizations.
        # Room: Stores information about hospital rooms.
        # Bill: Records information about invoices. 

        # Probably create methods create hospital where it creates the tables for the
        # Medicall staff, doctor, nurses ...
        # And another method create patient where it creates the tables for the patient
        # and the patient's data like medical history, insurance, emergency contact ...
        # And another method create hospitalization where it creates the tables for the
        # hospitalization and the rooms

        # N medical historys have a patient
        # A patient has n insurances, n emergency contacts, 
 
    def migrate(self):

        try:

            patient = {
                "idPatient": 0,
                "patient_fName": "John",
                "patient_lName": "Doe",
                "patient_bloodType": "A+",
                "patient_phone": "123456789",
                "patient_email": "",
                "patient_gender": "M",
                "patient_dob": "01/01/2000",
                "patient_history": [],
                # "patientEmergencyContact": [],
                "patientEpisodes": [],
                "InsurancePolicyNum": 0,
            }

            medicalHistory = {
                "idMedicalHistory": 0,
                "condition": "Covid",
                "recordDate": "01/01/2021",
            }

            insurance = {
                "policyId": 0,
                "provider": "Medicare",
                "plan": "A",
                "co_pay": 0,
                "coverage": "",
                "maternity": 0,
                "dental": 0,
                "optical": 0,
            }

            emergencyContact = {
                "idEmergencyContact": 0,
                "name": "Jane Doe",
                "relationship": "Mother",
                "phone": "987654321",
                "patientID": 0
            }

            cursor = self.OracleConnection.cursor()
            # Lets start by getting all the emergency contacts
            cursor.execute("SELECT COUNT(*) FROM EMERGENCY_CONTACT")
            emergencyContacts = cursor.fetchall()
            print(emergencyContacts)
            for row in cursor.execute("SELECT * FROM EMERGENCY_CONTACT"):
                print(row)
            print(emergencyContacts)

            #Lets get all the tables
            cursor.execute("select table_name from user_tables")
            tables = cursor.fetchall()
            print(tables)
            # Lets create the collection in mongo for the emergency contacts
            db = self.MongoConnection["BDNOSQLTP"]
            
            for emergencyContact in emergencyContacts:    
                emergencyContactPayload = {
                    "name": emergencyContact[0],
                    "relationship": emergencyContact[1],
                    "phone": emergencyContact[2],
                    "patientID": emergencyContact[3]
                }
                self.mongoDB["emergencyContact"].insert_one(emergencyContactPayload)

            # Lets create the collection in mongo for the medical history
            cursor.execute("SELECT * FROM MEDICAL_HISTORY")
            medicalHistories = cursor.fetchall()
            for medicalHistory in medicalHistories:
                medicalHistoryPayload = {
                    "recordId": medicalHistory[0],
                    "condition": medicalHistory[1],
                    "recordDate": medicalHistory[2],
                    "patientID": medicalHistory[3]
                }
                self.mongoDB["medicalHistory"].insert_one(medicalHistoryPayload)

            


        # Probably we wont use a list for the emergecny contacts for each user in the mongoDB since getting them from there is going to take more resources than 
        # Using another table to store them and then retrieving them by querying by the patientID


        except Exception as e:
            print("Error migrating from Oracle to MongoDB")
            print("Exception: ", e)

        

    def ensureOracle(self):
        sql_command=""
        try:
            cursor = self.OracleConnection.cursor()

            # cursor.execute("CREATE SCHEMA BDNOSQLTP;")
            print("Schema created in Oracle")
            # Lets execute the script hospotal.sql to create the tables
            file = open("./data/hospital.sql", "r")
            full_sql = file.read()
            # Split the script into individual statements
            sql_commands = full_sql.split(';')
            commandId = 0
            for sql_command in sql_commands:
                try:
                    #print(f"Executing command {commandId}")
                    cursor.execute(sql_command.strip())
   
                except Exception as e:
                    print("Error creating the table in Oracle")
                    print("Exception: ", e)
                    print("SQL Command: ", sql_command)

                commandId+=1

            # Lets get the procedures
            print("Tables created in Oracle")
            return True            
        
        except Exception as e:
            print("Error creating the table in Oracle")
            print("Exception: ", e)
            print("SQL Command: ", sql_command)
            return False
        

    # Probably not correct, but mongo creates the collection on the fly just by doing self.MongoConnection["BDNOSQLTP"]
    def ensureMongo(self):
        try:
            # In mongo I dont think that I have the need to create the database since I can retrieve it automatically

            self.mongoDB = self.MongoConnection["BDNOSQLTP"]
            return True
        except Exception as e:
            print("Error creating the collections in MongoDB")
            print("Exception: ", e)
            return False


    def connect(self, option):
        if (option == "Oracle" or option==0):
            try: 
                load_dotenv()

                user_db = os.getenv('ORACLE_USER')
                password_db = os.getenv('ORACLE_PASSWORD')

                connection = oracle.connect(
                    user=user_db, 
                    password=password_db, 
                    dsn="uxsn53y1lx5q8m5m_medium", 
                    wallet_location="./DBWallet", 
                    wallet_password=password_db, 
                    config_dir="./DBWallet"
                )


                print("Successfull connection")
                return connection
            except Exception as e:
                print('Error connecting to the database: ', e)
                return None
            
        if (option == "MongoDB" or option==1):
            try:
                client = pymongo.MongoClient("mongodb://localhost:27017/")
                
                print("Successfull connection")
                return client
            except:
                print('Error connecting to the database')
                return None

    def run_requirements(self):
        # Lets get all the installed pip packages
        out = os.system('pip freeze')
        # os.system('pip install -r requirements.txt')



mongoDBController()



# Connect to the atlas para ter triggers

# from pymongo.mongo_client import MongoClient
# from pymongo.server_api import ServerApi

# uri = "mongodb+srv://admin:bdnosql@cluster0.xmvpdfk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# # Create a new client and connect to the server
# client = MongoClient(uri, server_api=ServerApi('1'))

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)