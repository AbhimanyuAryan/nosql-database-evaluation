const changeEvent = {
    "operationType": "update",
    "fullDocument": {
        "_id": {
          "$oid": "665c8689b86a4232b284d792"
        },
        "patient_id": 2,
        "fname": "Jane",
        "lname": "Smith",
        "blood_type": "O-",
        "phone": "987-654-3210",
        "email": "jane.smith@example.com",
        "gender": "Female",
        "insurance": {
          "policy_number": "POL002",
          "provider": "XYZ Insurance",
          "insurance_type": "Premium Plan",
          "co_pay": 30,
          "coverage": "Partial Coverage",
          "maternity": "N",
          "dental": "Y",
          "vision": "Y"
        },
        "birthday": {
          "$date": "1990-03-20T00:00:00.000Z"
        },
        "medical_history": [
          {
            "medical_history_id": 2,
            "patient_id": "Allergy",
            "condition": {
              "$date": "2023-03-05T00:00:00.000Z"
            },
            "record_date": 2
          }
        ],
        "emergency_contacts": [
          {
            "name": "Jane Smith",
            "phone": "222-333-4444",
            "relation": "Mother"
          }
        ],
        "episodes": [
          {
            "episode_id": 181,
            "patient_id": 2,
            "prescriptions": [
              {
                "prescription_id": 199,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 30
              },
              {
                "prescription_id": 200,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 4
              },
              {
                "prescription_id": 201,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 16
              },
              {
                "prescription_id": 202,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 84
              },
              {
                "prescription_id": 203,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 74
              },
              {
                "prescription_id": 430,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 31
              },
              {
                "prescription_id": 431,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 60
              },
              {
                "prescription_id": 432,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 79
              }
            ],
            "bills": [],
            "screenings": [],
            "appointments": [],
            "hospitalizations": [
              {
                "admissionDate": {
                  "$date": "2020-05-04T00:00:00.000Z"
                },
                "dischargeDate": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "room": {
                  "room_id": 6,
                  "room_type": "VIP",
                  "room_cost": 300
                },
                "responsibleNurse": {
                  "employee_id": 26,
                  "fname": "Vickie",
                  "lname": "Gonzalez",
                  "dateJoining": {
                    "$date": "2018-04-10T00:00:00.000Z"
                  },
                  "dateSeparation": {
                    "$date": "2023-01-05T00:00:00.000Z"
                  },
                  "email": "lolson@example.com",
                  "address": "East Michael, CT 20442\"",
                  "department": {
                    "department_id": 16,
                    "dept_head": "Sophia Lopez",
                    "dept_name": "Ophthalmology",
                    "dept_employeeCount": 5
                  },
                  "ssn": 925078857,
                  "is_active": "N"
                }
              }
            ]
          }
        ]
      },
      "fullDocumentBeforeChange": {
        "_id": {
          "$oid": "665c8689b86a4232b284d792"
        },
        "patient_id": 2,
        "fname": "Jane",
        "lname": "Smith",
        "blood_type": "O-",
        "phone": "987-654-3210",
        "email": "jane.smith@example.com",
        "gender": "Female",
        "insurance": {
          "policy_number": "POL002",
          "provider": "XYZ Insurance",
          "insurance_type": "Premium Plan",
          "co_pay": 30,
          "coverage": "Partial Coverage",
          "maternity": "N",
          "dental": "Y",
          "vision": "Y"
        },
        "birthday": {
          "$date": "1990-03-20T00:00:00.000Z"
        },
        "medical_history": [
          {
            "medical_history_id": 2,
            "patient_id": "Allergy",
            "condition": {
              "$date": "2023-03-05T00:00:00.000Z"
            },
            "record_date": 2
          }
        ],
        "emergency_contacts": [
          {
            "name": "Jane Smith",
            "phone": "222-333-4444",
            "relation": "Mother"
          }
        ],
        "episodes": [
          {
            "episode_id": 181,
            "patient_id": 2,
            "prescriptions": [
              {
                "prescription_id": 199,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 30
              },
              {
                "prescription_id": 200,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 4
              },
              {
                "prescription_id": 201,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 16
              },
              {
                "prescription_id": 202,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 84
              },
              {
                "prescription_id": 203,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 74
              },
              {
                "prescription_id": 430,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 31
              },
              {
                "prescription_id": 431,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 60
              },
              {
                "prescription_id": 432,
                "prescription_date": {
                  "$date": "2020-05-15T00:00:00.000Z"
                },
                "medicine": {
                  "medicine_id": 1,
                  "medicine_name": "Paracetamol",
                  "medicine_quantity": 50,
                  "medicine_cost": 10
                },
                "dosage": 79
              }
            ],
            "bills": [],
            "screenings": [],
            "appointments": [],
            "hospitalizations": [
              {
                "admissionDate": {
                  "$date": "2020-05-04T00:00:00.000Z"
                },
                "dischargeDate": {
                  "$date": null
                },
                "room": {
                  "room_id": 6,
                  "room_type": "VIP",
                  "room_cost": 300
                },
                "responsibleNurse": {
                  "employee_id": 26,
                  "fname": "Vickie",
                  "lname": "Gonzalez",
                  "dateJoining": {
                    "$date": "2018-04-10T00:00:00.000Z"
                  },
                  "dateSeparation": {
                    "$date": "2023-01-05T00:00:00.000Z"
                  },
                  "email": "lolson@example.com",
                  "address": "East Michael, CT 20442\"",
                  "department": {
                    "department_id": 16,
                    "dept_head": "Sophia Lopez",
                    "dept_name": "Ophthalmology",
                    "dept_employeeCount": 5
                  },
                  "ssn": 925078857,
                  "is_active": "N"
                }
              }
            ]
          }
        ]
      },
    "ns": {
      "db": "your_database_name",
      "coll": "hospitalization"
    },
    "documentKey": {
      "_id": "665b29ddd8b06734b342ecc8"
    }
  }
  
  // Add a changeEvent to test it with your Trigger
  exports(changeEvent);
  


  exports = async function(changeEvent) {
    // A Database Trigger will always call a function with a changeEvent.
    // Documentation on ChangeEvents: https://docs.mongodb.com/manual/reference/change-events/
    console.log("Updated date in hospitalization");
    // This sample function will listen for events and replicate them to a collection in a different Database
    // Access the _id of the changed document:
    const docId = 0;
    console.log(changeEvent.operationType)
  
    // Get the MongoDB service you want to use (see "Linked Data Sources" tab)
    // Note: In Atlas Triggers, the service name is defaulted to the cluster name.
    const serviceName = "bdnosql";
    const database = "BDNOSQL";
    const collection = context.services.get(serviceName).db(database).collection(changeEvent.ns.coll);
  
    // Get the "FullDocument" present in the Insert/Replace/Update ChangeEvents
    try {
  
      // If this is an "update" or "replace" event, then replace the document in the other collection
      if (changeEvent.operationType === "update" || changeEvent.operationType === "replace") {
        
        var document = changeEvent.fullDocument;
        var documentBefore = changeEvent.fullDocumentBeforeChange;
        console.log("documentBefore");
        console.log(documentBefore);
        console.log("documentAfter\n");
        console.log(document);
        var flag = false;
        
  
        for (var i=0; i<documentBefore.episodes.length; i++) {
          
          for (var j=0; j<documentBefore.episodes[i].hospitalizations.length; j++) {
              var hospitalizationBefore = documentBefore.episodes[i].hospitalizations[j]; 
              var hospitalization = document.episodes[i].hospitalizations[j];
              if (hospitalizationBefore.dischargeDate["$date"] == null && hospitalization.dischargeDate["$date"] != null) {
                  flag = true;
  
                  var roomCost = hospitalization.room["room_cost"];
                  console.log("Room Cost: "+roomCost);
                  
                  var testCost = 0;
                  for (var k=0; k<documentBefore.episodes[i].screenings.length; k++) {
                      testCost += documentBefore.episodes[i].screenings[k].screening_cost;
                  }
                  console.log("Testing cost: "+testCost);
      
                  var otherCharges = 0;
                  for (var k=0; k<documentBefore.episodes[i].prescriptions.length; k++) {
                      otherCharges += documentBefore.episodes[i].prescriptions[k].medicine.medicine_cost * documentBefore.episodes[i].prescriptions[k].dosage;
                  }
                  console.log("Prescription Charges: "+otherCharges);
      
    
                  var totalCost = roomCost + testCost + otherCharges;
                  
                  console.log("totalCost");
                  console.log(totalCost);
    
                  await collection.insertOne({
                      idepisode: document.idepisode,
                      room_cost: roomCost,
                      test_cost: testCost,
                      other_charges: otherCharges,
                      total: totalCost,
                      payment_status: "PENDING",
                      registered_at: new Date()
                  });
                  
                  console.log("Cost of the room updated to: "+totalCost);
              }
            
        }
        if (!flag)
          await collection.replaceOne({"_id": docId}, changeEvent.fullDocument);
      }
      
      console.log("Succesfully updated the document");
      }
    } catch(err) {
      console.log("error performing mongodb write: ", err.message);
    }
  };