--------------------------------------------------------
--  Ref Constraints for Table EMERGENCY_CONTACT
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."EMERGENCY_CONTACT" ADD CONSTRAINT "FK_EMERGENCY_CONTACT_PATIENT1" FOREIGN KEY ("IDPATIENT")
	  REFERENCES "C##ABHI"."PATIENT" ("IDPATIENT") ENABLE;
