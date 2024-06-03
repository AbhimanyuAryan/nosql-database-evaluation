--------------------------------------------------------
--  Ref Constraints for Table EPISODE
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."EPISODE" ADD CONSTRAINT "FK_EPISODE_PATIENT1" FOREIGN KEY ("PATIENT_IDPATIENT")
	  REFERENCES "C##ABHI"."PATIENT" ("IDPATIENT") ENABLE;
