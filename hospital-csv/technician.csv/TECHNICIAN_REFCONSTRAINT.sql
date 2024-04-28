--------------------------------------------------------
--  Ref Constraints for Table TECHNICIAN
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."TECHNICIAN" ADD CONSTRAINT "FK_TECHNICIAN_STAFF1" FOREIGN KEY ("STAFF_EMP_ID")
	  REFERENCES "C##ABHI"."STAFF" ("EMP_ID") ENABLE;
