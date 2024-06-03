--------------------------------------------------------
--  Ref Constraints for Table NURSE
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."NURSE" ADD CONSTRAINT "FK_NURSE_STAFF1" FOREIGN KEY ("STAFF_EMP_ID")
	  REFERENCES "C##ABHI"."STAFF" ("EMP_ID") ENABLE;
