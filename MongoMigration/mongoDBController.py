import os
import oracledb as oracle
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import re
import json
import requests
from requests.auth import HTTPDigestAuth

import sys
sys.path.append('./structs/')
from structs import *
from tqdm import tqdm

class mongoDBController():
    def __init__(self):
        #self.run_requirements()
        self.OracleConnection = self.connect(option="Oracle")
        self.MongoConnection = self.connect(option="MongoDB")  
        if self.OracleConnection!=None and self.MongoConnection!=None:
            #self.dropDBs()
            self.ensureDBs()
            #self.migrate()
            #Lets create the trigger in MongoDB
            triggerFunction = """
                exports = function(event) {
                const { fullDocument } = event;

                // Check if the discharge date has been updated
                if (fullDocument && !fullDocument.discharge_date) {
                    const { room_idroom, idepisode } = fullDocument;

                    // Calculate the room cost for the associated hospitalization
                    const roomCost = db.room.aggregate([
                    { $match: { idroom: room_idroom } },
                    { $group: { _id: null, totalRoomCost: { $sum: "$room_cost" } } }
                    ]).toArray()[0].totalRoomCost || 0;

                    // Calculate the test cost for the associated hospitalization
                    const testCost = db.lab_screening.aggregate([
                    { $match: { episode_idepisode: idepisode } },
                    { $group: { _id: null, totalTestCost: { $sum: "$test_cost" } } }
                    ]).toArray()[0].totalTestCost || 0;

                    // Calculate the other charges for prescriptions for the associated hospitalization
                    const otherCharges = db.prescription.aggregate([
                    { $match: { idepisode: idepisode } },
                    { $lookup: { from: "medicine", localField: "idmedicine", foreignField: "idmedicine", as: "medicine" } },
                    { $unwind: "$medicine" },
                    { $group: { _id: null, totalOtherCharges: { $sum: { $multiply: ["$medicine.m_cost", "$dosage"] } } } }
                    ]).toArray()[0].totalOtherCharges || 0;

                    // Calculate the total cost of the bill for the associated episode
                    const totalCost = roomCost + testCost + otherCharges;

                    // Insert the bill with the total cost for the associated episode
                    db.bill.insertOne({
                    idepisode: idepisode,
                    room_cost: roomCost,
                    test_cost: testCost,
                    other_charges: otherCharges,
                    total: totalCost,
                    payment_status: "PENDING",
                    registered_at: new Date()
                    });
                }
                };
            """
            self.createAtlasFunction({"name": self.sqltriggers[0][0], "code": triggerFunction})

            self.OracleConnection.close()
            self.MongoConnection.close()
            print("Migration Completed")
        else:
            print("Migration Failed")

    def dropDBs(self):
        #Drop the Oracle database
        try:
            cursor = self.OracleConnection.cursor()
            #Lets get all the table names from the hospital.sql file
            file = open("./data/hospital.sql", "r")
            full_sql = file.read()
            regexTables = re.findall(r"CREATE TABLE (\w+)", full_sql)
            for table in regexTables:
                try:
                    cursor.execute("DROP TABLE " + table + " CASCADE CONSTRAINTS")
                except Exception as e:
                    pass

            regexProcedures = re.findall(r"CREATE OR REPLACE PROCEDURE (\w+)", full_sql)
            for procedure in regexProcedures:
                try:
                    cursor.execute("DROP PROCEDURE " + procedure)
                except Exception as e:
                    pass
            
            regexFunctions = re.findall(r"CREATE OR REPLACE FUNCTION (\w+)", full_sql)
            for function in regexFunctions:
                try:
                    cursor.execute("DROP FUNCTION " + function)
                except Exception as e:
                    pass
            regexTriggers = re.findall(r"CREATE OR REPLACE TRIGGER (\w+)", full_sql)
            for trigger in regexTriggers:
                try:
                    cursor.execute("DROP TRIGGER " + trigger)
                except Exception as e:
                    pass
                
            sequenceRegex = re.findall(r"CREATE SEQUENCE (\w+)", full_sql)
            for sequence in sequenceRegex:
                try:
                    cursor.execute("DROP SEQUENCE " + sequence)
                except Exception as e:
                    pass

            viewRegex = re.findall(r"CREATE VIEW (\w+)", full_sql)
            for view in viewRegex:
                try:
                    cursor.execute("DROP VIEW " + view)
                except Exception as e:
                    pass

            print("Oracle database dropped")

            #Lets drop the mongo database
            self.MongoConnection.drop_database("BDNOSQLTP")
            print("MongoDB database dropped")

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
            self.ensureMongo()
            return True
        except Exception as e:
            print("Error creating the databases")
            print("Exception: ", e)
            return False
        
    def migrate(self):

        try:
            
            #Lets get all the patients
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

                    
                    #Lets get the medical history of the patient   
                    cursor.execute(f"SELECT * FROM MEDICAL_HISTORY WHERE IDPATIENT = {patientID}")
                    medical_histories = cursor.fetchall()
                    medical_histories_list = []
                    for medical_history in medical_histories:
                        try:
                            medical_histories_list.append(MedicalHistory(medical_history[0], medical_history[1], medical_history[2], medical_history[3]))
                            

                        except Exception as e:
                            pass
                    
                    #Lets get the insurance of the patient
                    
                    cursor.execute(f"SELECT * FROM INSURANCE WHERE policy_number='{patientPolicyNumber}'")
                    insurance = cursor.fetchone()
                    
                    try:
                        insurancePolNum = insurance[0]
                        insuranceProvider = insurance[1]
                        insuranceType = insurance[2]
                        insuranceCoPay = insurance[3]
                        insuranceCoverage = insurance[4]
                        insuranceMaternity = insurance[5]
                        insuranceDental = insurance[6]
                        insuranceVision = insurance[7]

                        patientInsurance = Insurance(insurancePolNum, insuranceProvider, insuranceType, insuranceCoPay, insuranceCoverage, insuranceMaternity, insuranceDental, insuranceVision)
                    except Exception as e:
                        print("Error getting the insurance of the patient")
                        print("Exception: ", e)
                    
                    #Lets get the emergency contacts of the patient
                    cursor.execute(f"SELECT * FROM EMERGENCY_CONTACT WHERE IDPATIENT = {patientID}")
                    emergency_contacts = cursor.fetchall()
                    emergency_contacts_list = []
                    for emergency_contact in emergency_contacts:
                        try:
                            emergencyContactName = emergency_contact[0]
                            emergencyContactPhone = emergency_contact[1]
                            emergencyContactRelation = emergency_contact[2]
                            emergency_contacts_list.append(EmergencyContact(emergencyContactName, emergencyContactPhone, emergencyContactRelation))
                        except Exception as e:
                            pass
                    
                    #Lets get the patients episodes
                    cursor.execute(f"SELECT * FROM EPISODE WHERE PATIENT_IDPATIENT = {patientID}")
                    episodes = cursor.fetchall()
                    episodesList = []
                    for episode in episodes:
                        episodeId = episode[0]
                        #Lets get the prescriptions of the episode
                        cursor.execute(f"SELECT * FROM PRESCRIPTION WHERE IDEPISODE = {episodeId}")
                        prescriptions = cursor.fetchall()
                        prescriptions_list = []
                        for prescription in prescriptions:
                            try:
                                prescriptionId = prescription[0]
                                prescriptionDate = prescription[1]
                                prescriptionMedicine = prescription[3]
                                prescriptionDosage = prescription[2]

                                #Lets get the medicine data
                                cursor.execute(f"SELECT * FROM MEDICINE WHERE IDMEDICINE = 1")
                                medicine = cursor.fetchone()
                                prescriptionMedicine = Medicine(medicine[0], medicine[1], medicine[2], medicine[3])

                                prescriptions_list.append(Prescription(prescriptionId, prescriptionDate, prescriptionMedicine, prescriptionDosage))
                            except Exception as e:
                                print("Error getting the prescriptions of the episode")
                        
                        #Lets get the bills of the episode
                        cursor.execute(f"SELECT * FROM BILL WHERE IDEPISODE = {episodeId}")
                        bills = cursor.fetchall()
                        bills_list = [] 
                        for bill in bills:
                            billId = bill[0]
                            billRoomCost = bill[1]
                            billTestCost = bill[2]
                            billAddCharges = bill[3]
                            billTotalCost = bill[4]
                            billRegisterDate = bill[6]
                            billPaymentStatus = bill[7]

                            bills_list.append(Bill(billId, billRoomCost, billTestCost, billAddCharges, billTotalCost, billRegisterDate, billPaymentStatus))

                        #Lets get the episodes screenings
                        cursor.execute(f"SELECT * FROM LAB_SCREENING WHERE EPISODE_IDEPISODE = {episodeId}")
                        screenings = cursor.fetchall()
                        screenings_list = []
                        for screening in screenings:
                            try:
                                screeningId = screening[0]
                                screeningCost = screening[1]
                                screeningDate = screening[2]
                                screeningTechnicianId = screening[3]

                                #Lets get the technician data
                                cursor.execute(f"SELECT STAFF_EMP_ID FROM TECHNICIAN WHERE IDTECHNICIAN = {screeningTechnicianId}")
                                technician = cursor.fetchone()
                                screeningTechnicianId = technician[0]

                                #Lets get the technician data by querying the staff table
                                cursor.execute(f"SELECT * FROM STAFF WHERE EMP_ID = {screeningTechnicianId}")
                                technician = cursor.fetchone()
                                
                                technician = Employee(technician[0], technician[1], technician[2], technician[3], technician[4], technician[5], technician[6], technician[7], technician[8])


                                screenings_list.append(LabScreening(screeningId, screeningCost, screeningDate, technician))
                            except Exception as e:
                                pass

                        #Lets get the episodes appointment
                        cursor.execute(f"SELECT * FROM APPOINTMENT WHERE IDEPISODE = {episodeId}")
                        appointments = cursor.fetchall()
                        appointments_list = []
                        for appointment in appointments:
                            try:
                                appointmentId = appointment[0]
                                appointmentDate = appointment[1]
                                appointmentDoctorId = appointment[2]
                                appointmentRoomId = appointment[3]
                                appointmentStatus = appointment[4]

                                #Lets get the doctor data
                                cursor.execute(f"SELECT STAFF_EMP_ID FROM DOCTOR WHERE IDDOCTOR = {appointmentDoctorId}")
                                doctor = cursor.fetchone()
                                appointmentDoctorId = doctor[0]

                                #Lets get the doctor data by querying the staff table
                                cursor.execute(f"SELECT * FROM STAFF WHERE EMP_ID = {appointmentDoctorId}")
                                doctor = cursor.fetchone()
                                
                                doctor = Employee(doctor[0], doctor[1], doctor[2], doctor[3], doctor[4], doctor[5], doctor[6], doctor[7], doctor[8])
                                #scheduled_on, appointment_date, appointment_time, doctor
                                appointments_list.append(Appointment(appointmentId, appointmentDate, doctor, appointmentStatus))
                            except Exception as e:
                                pass
                        
                        #Lets get all the hospitalizations of the episode
                        cursor.execute(f"SELECT * FROM HOSPITALIZATION WHERE IDEPISODE = {episodeId}")
                        hospitalizations = cursor.fetchall()
                        hospitalizations_list = []
                        for hospitalization in hospitalizations:
                            try:
                                hospitalizationId = hospitalization[0]
                                hospitalizationAdmissionDate = hospitalization[1]
                                hospitalizationDischargeDate = hospitalization[2]
                                hospitalizationRoomId = hospitalization[3]
                                hospitalizationResponsibleNurseId = hospitalization[4]

                                #Lets get the nurse data
                                cursor.execute(f"SELECT STAFF_EMP_ID FROM NURSE WHERE IDNURSE = {hospitalizationResponsibleNurseId}")
                                nurse = cursor.fetchone()
                                hospitalizationResponsibleNurseId = nurse[0]

                                #Lets get the nurse data by querying the staff table
                                cursor.execute(f"SELECT * FROM STAFF WHERE EMP_ID = {hospitalizationResponsibleNurseId}")
                                nurse = cursor.fetchone()
                                
                                nurse = Employee(nurse[0], nurse[1], nurse[2], nurse[3], nurse[4], nurse[5], nurse[6], nurse[7], nurse[8])
                                

                                #Lets get the room data
                                cursor.execute(f"SELECT * FROM ROOM WHERE IDROOM = {hospitalizationRoomId}")
                                room = cursor.fetchone()
                                room = Room(room[0], room[1], room[2], room[3], room[4])

                                hospitalizations_list.append(Hospitalization(hospitalizationId, hospitalizationAdmissionDate, hospitalizationDischargeDate, room, nurse))
                            except Exception as e:
                                pass

                        episodesList.append(Episode(episodeId, patientID, prescriptions_list, bills_list, screenings_list, appointments_list, hospitalizations_list))
                      
                    #Lets conver the Patient to  a json object and insert it into the MongoDB
                    patient = Patient(patientID, patientFname, patientLname, patientBloodType, patientPhone, patientEmail, patientGender, patientPolicyNumber, patientBirthday, medical_histories_list, patientInsurance, emergency_contacts_list, episodesList)
                    patient = patient.to_json()
                    self.mongoDB["Patient"].insert_one(patient)

                except Exception as e:
                    print("Error migrating patient")
                    print("Exception: ", e)



        # Probably we wont use a list for the emergecny contacts for each user in the mongoDB since getting them from there is going to take more resources than 
        # Using another table to store them and then retrieving them by querying by the patientID


        except Exception as e:
            print("Error migrating from Oracle to MongoDB")
            print("Exception: ", e)

    def ensureOracle(self):
        sql_command=""
        try:
            cursor = self.OracleConnection.cursor()


            # Lets execute the script hospotal.sql to create the tables
            file = open("./data/hospital.sql", "r")
            full_sql = file.read()

            #Lets capture the procedures and triggers from the script in order to then execute them in the mongo db
            proceduresRegex = r"CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+(\w+)\s*\(([^)]*)\)\s*IS\s*([\s\S]*)END;\s*\/"
            triggersRegex = r"CREATE\s+OR\s+REPLACE\s+TRIGGER\s+(\w+)\sAFTER\s(INSERT|UPDATE|DELETE)\sOF\s(\w+)\sON\s(\w+)\sFOR\sEACH\sROW\s*DECLARE(\s+.*\s)+BEGIN\s*([\s\S]*)END;\s*\/"

            self.sqlprocedures = re.findall(proceduresRegex, full_sql)
            self.sqltriggers = re.findall(triggersRegex, full_sql)
            
            #Lets verify if the tables already exist if so return
            regexTables = re.findall(r"CREATE TABLE (\w+)", full_sql)
            for table in regexTables:
                try:
                    cursor.execute(f"SELECT * FROM {table}")
                    print("Tables already created in Oracle. Skipping...")
                    return True
                except Exception as e:
                    pass

            #Lets strip them from the full_sql
            full_sql = re.sub(proceduresRegex, "", full_sql)
            full_sql = re.sub(triggersRegex, "", full_sql)
            #Lets capture all the comments from the script
           # comments = re.findall(r"/\*+[\s\S]*?\*+/", full_sql)
            full_sql = re.sub(r"/\*+[\s\S]*?\*+/", "", full_sql)

            # Split the script into individual statements
            sql_commands = full_sql.split(';')
            numCommands = len(sql_commands)
            commandId = 0
            for i in tqdm(range(numCommands), desc="Running SQL commands", unit="command"):
                try:
                    sql_command = sql_commands[i]
                    cursor.execute(sql_command.strip())
                except Exception as e:
                    pass

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
        
    def createTrigger(self, trigger, functionId):
        #https://www.mongodb.com/docs/atlas/app-services/admin/api/v3/#tag/triggers/operation/adminCreateTrigger
        try:
            groupId = 0
            appId = 0
            uri = f"https://services.cloud.mongodb.com/api/admin/v3.0/groups/{groupId}/apps/{appId}/triggers"

            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer <token>"
            }

            payload = {
                "name": trigger["name"],
                "type": "DATABASE",
                "function_id": functionId,
                "config": {
                    "operation_types": trigger["operations"],
                    "database": trigger["database"],
                    "collection": trigger["collection"],
                    "service_id": "5adeb649b8b998486770ae7c",
                    "match": {},
                    "project": {},
                    "full_document": True
                }
            }

            response = requests.post(uri, headers=headers, json=payload)

            if response.status_code == 201:
                print("Trigger created in MongoDB")
            else:
                print("Error creating the trigger in MongoDB")
                print("Response: ", response.json())

        except Exception as e:
            print("Error creating the trigger in MongoDB")
            print("Exception: ", e)


    def createAtlasFunction(self, function):
        #https://www.mongodb.com/docs/atlas/app-services/admin/api/v3/#tag/functions/operation/adminCreateFunction
        try:
            projectId = self.mongoGroupId
            groupId = self.mongoProjectId
            uri = f"https://services.cloud.mongodb.com/api/atlas/v1.0/groups/{groupId}/apps/{appId}/functions"
            """
            headers = {
                'Content-Type': 'application/json',
                'Access-Control-Request-Headers': '*',
                'api-key': self.mongoBearer,
                'Accept': 'application/json'
            }

            payload = {
                "name": function["name"],
                "private": True,
                "source": function["code"],
                "run_as_system": True
                }
            
            print("Headers: ", headers)
            #Response:
            response = requests.post(uri, headers=headers, json=payload)
            #{
            #"_id": "string",
            #"name": "string"
            #}
            if response.status_code == 201:
                print("Function created in MongoDB")
                return response.json()
            else:
                print("Error creating the function in MongoDB")
                print("Response: ", response.json())
            """
            auth = HTTPDigestAuth('cpfdagvq', 'b08b1acc-b0c0-4d3c-8004-235e4a18619b')
            payload = json.dumps({
                "collection": "Patient",
                "database": "bdnosql",
                "dataSource": "bdnosql",
                "projection": {
                    "_id": 1
                }
            })

            headers = {"Accept"       : "application/vnd.atlas.2023-01-01+json",
                    "Content-Type" : "application/json"}

            url = f"https://cloud.mongodb.com/api/atlas/v2/groups"
            response = requests.get(url, auth=auth, headers=headers)

            print(response.text)
        except Exception as e:
            print("Error creating the function in MongoDB")
            print("Exception: ", e)
            return None


    def connect(self, option):
        if (option == "Oracle" or option==0):
            try: 
                load_dotenv()

                user_db = os.getenv('ORACLE_USER')
                password_db = os.getenv('ORACLE_PASSWORD')

                connection = oracle.connect(user=user_db, 
                                            mode=oracle.AUTH_MODE_SYSDBA,
                                            password=password_db,
                                            host="localhost", 
                                            port=1521, 
                                            service_name="xe"
                                        )


                print("Successfull connection to Oracle Database")
                return connection
            except Exception as e:
                print('Error connecting to the database: ', e)
                return None
            
        if (option == "MongoDB" or option==1):
            try:
                mongoUserName = os.getenv('MONGO_USER') 
                mongoPassword = os.getenv('MONGO_PASSWORD')
                uri = f"mongodb+srv://{mongoUserName}:{mongoPassword}@bdnosql.isx6fkl.mongodb.net/?retryWrites=true&w=majority&appName=bdnosql"

                client = MongoClient(uri, server_api=ServerApi('1'))
                client.admin.command('ping')

                self.mongoBearer = os.getenv('MONGO_BEARER')
                self.mongoProjectId = os.getenv('MONGO_PROJECT_ID')
                self.mongoGroupId = os.getenv('MONGO_GROUP_ID')
                
                print("Successfull connection to MongoDB Database")
                return client
            except:
                print('Error connecting to the database')
                return None

    def run_requirements(self):
        # Lets get all the installed pip packages
        os.system('pip install -r requirements.txt')



mongoDBController()