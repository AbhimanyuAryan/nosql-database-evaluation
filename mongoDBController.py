import os
import oracledb as oracle
from dotenv import load_dotenv
import pymongo

class mongoDBController():
    def __init__(self):
        self.run_requirements()
        self.OracleConnection = self.connect(option="Oracle")
        self.MongoConnection = self.connect(option="MongoDB")  
        if self.OracleConnection!=None and self.MongoConnection!=None:
            # self.ensureDBs()
            self.OracleConnection.close()
            self.MongoConnection.close()
            print("Migration Completed")
        else:
            print("Migration Failed")

    def migrate(self):
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

        pass


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
        

    def ensureOracle(self):
        try:
            cursor = self.OracleConnection.cursor()
            cursor.execute("SHOW SCHEMAS;")
            result = False
            for i in cursor:
                if i[0] == "BDNOSQLTP":
                    result=True

            if not result:
                cursor.execute("CREATE SCHEMA BDNOSQLTP;")
                print("Schema created in Oracle")
                # Lets execute the script hospotal.sql to create the tables
                with open('hospital.sql', 'r') as file:
                    sql = file.read()
                    cursor.execute(sql)
                print("Tables created in Oracle")

            return True            
        
        except Exception as e:
            print("Error creating the table in Oracle")
            print("Exception: ", e)
            return False
        

    # Probably not correct, but mongo creates the collection on the fly just by doing self.MongoConnection["BDNOSQLTP"]
    def ensureMongo(self):
        try:
            dblist = self.MongoConnection.list_database_names()
            if "BDNOSQLTP" in dblist:
                print("The database exists.")

            return True
        except Exception as e:
            print("Error creating the collections in MongoDB")
            print("Exception: ", e)
            return False


    def connect(self, option):
        if (option == "Oracle" or option==0):
            try: 
                load_dotenv()
                wpassword = os.getenv('ORACLE_WALLET_PASSWORD')
                password = os.getenv('ORACLE_DB_PASSWORD')
                wallet_dir = os.getenv('ORACLE_WALLET_PATH')

                connection = oracle.connect(
                    config_dir=wallet_dir,
                    user="admin",
                    password=password,
                    dsn="bdnosqltp_tp",
                    wallet_location=wallet_dir,
                    wallet_password=wpassword
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
        os.system('pip install -r requirements.txt')



mongoDBController()