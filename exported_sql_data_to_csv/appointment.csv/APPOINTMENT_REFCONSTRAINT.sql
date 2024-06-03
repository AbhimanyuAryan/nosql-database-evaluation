--------------------------------------------------------
--  Ref Constraints for Table APPOINTMENT
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."APPOINTMENT" ADD CONSTRAINT "APPOINTMENT_DOCTOR_FK" FOREIGN KEY ("IDDOCTOR")
	  REFERENCES "C##ABHI"."DOCTOR" ("EMP_ID") ENABLE;
  ALTER TABLE "C##ABHI"."APPOINTMENT" ADD CONSTRAINT "FK_APPOINTMENT_EPISODE1" FOREIGN KEY ("IDEPISODE")
	  REFERENCES "C##ABHI"."EPISODE" ("IDEPISODE") ENABLE;