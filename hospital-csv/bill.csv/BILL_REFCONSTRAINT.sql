--------------------------------------------------------
--  Ref Constraints for Table BILL
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."BILL" ADD CONSTRAINT "FK_BILL_EPISODE1" FOREIGN KEY ("IDEPISODE")
	  REFERENCES "C##ABHI"."EPISODE" ("IDEPISODE") ENABLE;
