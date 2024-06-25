# Comparing Oracle/MongoDB/Noe4j database

The aim of this practical is to carry out a work of analysis, planning, implementation of a relation and two non-relational DBMS, MongoDB and Neo4j. Migrated Oracle database to MongoDB and Graph Database


## Oracle to Neo4j migration

### Better DB structure 

Based on feedback from the presentation by the professor, we have revised the script in the attached project zip: **neo4j/improvised_sql2_neo4j.py**.

The updated approach consolidates Doctor, Nurse, and Technician roles into a unified 'Staff' category and eliminates unnecessary relationships. The new relationships are now defined as follows:

- Hospitalization → **RESPONSIBLE_FOR** → Staff
- Appointment → **HAS_DOCTOR** → Staff
- Lab_Screening → **PERFORMED_BY** → Staff

### Further Architectural design considerations and rational behind those considerations

#### Removing Episode

Episode is basically the connector between multiple nodes and doesn't really contain any data. So the intuition for a simplified design was to remove the node and establish direct relation 

But it doesn't specifically contain any useful data. But that would be bad approach for various reasons


### Comparing triggers between MongoDB and Neo4j

Since triggers are not part of Neo4j by default, we had to enable them by using the *APOC* (Awesome Procedures on Cypher) library.

Based on the documentation provided on the GitHub repo, you need to enable them in the `apoc.conf` file in the same directory as `neo4j.conf` with the following configuration settings:

- `apoc.trigger.enabled=true`
- `apoc.trigger.refresh=60000`

We have defined the following triggers:

#### Trigger for Logging New Patient Creation
```cypher
CALL apoc.trigger.add(
'trg_generate_bill',
'
UNWIND $createdNodes AS n
WITH n
WHERE n:Hospitalization AND n.DISCHARGE_DATE IS NOT NULL
MATCH (n)-[:INVOLVES]->(e:Episode),
    (n)-[:ASSIGNED_TO]->(r:Room),
    (p:Prescription)-[:PRESCRIBED_FOR]->(e),
    (m:Medicine)<-[:PRESCRIBED_MEDICINE]-(p),
    (ls:Lab_Screening)-[:BELONGS_TO]->(e)
WITH n, r, p, m, ls, SUM(r.ROOM_COST) AS v_room_cost, SUM(ls.TEST_COST) AS v_test_cost, SUM(m.M_COST * p.DOSAGE) AS v_other_charges
WITH n, v_room_cost, v_test_cost, v_other_charges, v_room_cost + v_test_cost + v_other_charges AS v_total_cost
CREATE (b:Bill {
    episodeId: n.IDEPISODE,
    roomCost: v_room_cost,
    testCost: v_test_cost,
    otherCharges: v_other_charges,
    total: v_total_cost,
    paymentStatus: "PENDING",
    registeredAt: datetime()
})
RETURN n
',
{phase:'after'}
);
```

```js
exports = async function(changeEvent) {
    const docId = 0;
    const serviceName = "bdnosql";
    const database = "BDNOSQL";
    const collection = context.services.get(serviceName).db(database).collection("Patient");
  
    try {
      if (changeEvent.operationType === "update") {
        const document = changeEvent.fullDocument;
        const documentBefore = changeEvent.fullDocumentBeforeChange;
        let flag = false;
  
        for (let i = 0; i < documentBefore.episodes.length; i++) {
          for (let j = 0; j < documentBefore.episodes[i].hospitalizations.length; j++) {
            const hospitalizationBefore = documentBefore.episodes[i].hospitalizations[j];
            const hospitalization = document.episodes[i].hospitalizations[j];
            if (hospitalizationBefore.dischargeDate == null && hospitalization.dischargeDate != null) {
              flag = true;
  
              let roomCost = hospitalization.room.room_cost;
              let testCost = 0;
              let otherCharges = 0;
              
              for (let k = 0; k < documentBefore.episodes[i].screenings.length; k++) {
                testCost += documentBefore.episodes[i].screenings[k].screening_cost;
              }
  
              for (let k = 0; k < documentBefore.episodes[i].prescriptions.length; k++) {
                const prescription = documentBefore.episodes[i].prescriptions[k];
                otherCharges += prescription.medicine.medicine_cost * prescription.dosage;
              }
  
              let totalCost = roomCost + testCost + otherCharges;
  
              const billPayload = {
                "bill_id": 1,
                "room_cost": roomCost,
                "test_cost": testCost,
                "other_charges": otherCharges,
                "total_cost": totalCost,
                "register_date": new Date(),
                "payment_status": "PENDING"
              };
  
              
  
              document.episodes[i].bills.push(billPayload);
              console.log("Document _id: "+docId);
              console.log("episodes."+2+".bills");
              await collection.updateOne(
                { "patient_id" :  2 }, 
                { "$push" : { 
                  "episodes.2.bills" : billPayload
                  } 
                }
              )
              .then(result => {
                console.log("Successfully updated document");
              })
              .catch(error => {
                console.error("Error updating document:", error);
              });
              console.log("Updated bill with values:");
              console.log("Room Cost: "+ roomCost);
              console.log("Testing Cost: "+testCost);
              console.log("Aditional Charges: "+ otherCharges);
              console.log("Total cost of the episode: "+totalCost);
              console.log("Successfully inserted bill for episode: " + document.episodes[i].episode_id);
            }
          }
        }
  
        if (!flag) {
          await collection.replaceOne({ "_id": docId }, document);
          console.log("Document replaced successfully");
        }
      } else {
        console.log("Unsupported operation type: " + changeEvent.operationType);
      }
    } catch(err) {
      console.error("Error performing MongoDB write: ", err.message);
    }
  };
```


### Comparing queries between Mongodb and Neo4j

```
MATCH (p1:Patient {IDPATIENT: 1}), (p2:Patient {IDPATIENT: 2}),
            path = shortestPath((p1)-[*]-(p2))
        RETURN path
```
<img width="273" alt="Screenshot 2024-06-25 at 17 23 13" src="https://github.com/AbhimanyuAryan/nosql-database-evaluation/assets/8083613/009eaee7-f463-411e-af22-1705b7a65666">

### Comparing views between Mongodb and Neo4j

```
self.mongoDB.create_collection(
        viewName,
        viewOn = "Patient",
        pipeline=[
        {
            '$unwind': {
                'path': '$episodes'
            }
        }, {
            '$unwind': {
                'path': '$episodes.appointments'
            }
        }, {
            '$project': {
                'appointment_scheduled_date': '$episodes.appointments.scheduled_on', 
                'appointment_date': '$episodes.appointments.appointment_date', 
                'appointment_time': '$episodes.appointments.appointment_time', 
                'doctor_id': '$episodes.appointments.doctor.employee.employee_id', 
                'doctor_qualifications': '$episodes.appointments.doctor.qualifications', 
                'department_name': '$episodes.appointments.doctor.employee.department.dept_name', 
                'patient_first_name': '$fname', 
                'patient_last_name': '$lname', 
                'patient_blood_type': '$blood_type', 
                'patient_phone': '$phone', 
                'patient_email': '$email', 
                'patient_gender': '$gender'
            }
        }
        ])
```

Neo4J doesn’t have views in the way that relational DBs have. There are several things we could do
as alternates:
* Continually re-issue the query that computes the ”view”you need, as needed
* Create a special ”view node”, and then link that node via relationships to all of the other nodes
that would naturally occur in your ”view”. Querying your view then becomes as simple as pulling
up that one ”view node”and traversing your edges to the view results.
Option 1 is easiest, option 2 is probably faster, but comes with it the maintenance burden that
as your underlying nodes in the DB change, you need to maintain your view and make sure it points
to the right places.

#### Create a view node named 'Summary'

```python
CREATE (:Summary {view_name: 'Overall Summary'})

MATCH (summary:Summary), (patient:Patient)
WHERE EXISTS((patient)-[:HAS_EPISODE]->())
MERGE (summary)-[:INCLUDES_PATIENT]->(patient)

MATCH (summary:Summary)-[:INCLUDES_PATIENT]->(patient:Patient)
RETURN summary, patient
```

