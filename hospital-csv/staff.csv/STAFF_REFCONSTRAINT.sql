--------------------------------------------------------
--  Ref Constraints for Table STAFF
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."STAFF" ADD CONSTRAINT "FK_STAFF_DEPARTMENT1" FOREIGN KEY ("IDDEPARTMENT")
	  REFERENCES "C##ABHI"."DEPARTMENT" ("IDDEPARTMENT") ENABLE;
