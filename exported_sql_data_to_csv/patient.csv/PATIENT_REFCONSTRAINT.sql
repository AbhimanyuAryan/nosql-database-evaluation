--------------------------------------------------------
--  Ref Constraints for Table PATIENT
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."PATIENT" ADD CONSTRAINT "FK_PATIENT_INSURANCE" FOREIGN KEY ("POLICY_NUMBER")
	  REFERENCES "C##ABHI"."INSURANCE" ("POLICY_NUMBER") ENABLE;
