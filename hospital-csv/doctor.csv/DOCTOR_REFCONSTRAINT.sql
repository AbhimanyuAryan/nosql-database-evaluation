--------------------------------------------------------
--  Ref Constraints for Table DOCTOR
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."DOCTOR" ADD CONSTRAINT "FK_DOCTOR_STAFF1" FOREIGN KEY ("EMP_ID")
	  REFERENCES "C##ABHI"."STAFF" ("EMP_ID") ENABLE;
