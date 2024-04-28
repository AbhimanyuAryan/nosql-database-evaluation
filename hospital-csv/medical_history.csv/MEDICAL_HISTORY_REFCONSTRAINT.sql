--------------------------------------------------------
--  Ref Constraints for Table MEDICAL_HISTORY
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."MEDICAL_HISTORY" ADD CONSTRAINT "FK_MEDICAL_HISTORY_PATIENT1" FOREIGN KEY ("IDPATIENT")
	  REFERENCES "C##ABHI"."PATIENT" ("IDPATIENT") ENABLE;
