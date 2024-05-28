import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

def test_neo4j_connection(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            result = session.run("RETURN 1")
            for record in result:
                print(record)
    finally:
        driver.close()

if __name__ == "__main__":
    # Load the environment variables from dados.env using a raw string
    load_dotenv(r'C:\Users\anari\Desktop\nosql-database-evaluation\NEO4J\dados.env')
    
    # Debugging: Print environment variables to ensure they are loaded
    print(f"NEO4J_URI: {os.getenv('NEO4J_URI')}")
    print(f"NEO4J_USER: {os.getenv('NEO4J_USER')}")
    print(f"NEO4J_PASSWORD: {os.getenv('NEO4J_PASSWORD')}")
    
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    
    if not uri or not user or not password:
        print("One or more environment variables are missing.")
    else:
        test_neo4j_connection(uri, user, password)
