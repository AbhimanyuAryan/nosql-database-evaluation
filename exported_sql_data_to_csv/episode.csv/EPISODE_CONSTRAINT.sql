--------------------------------------------------------
--  Constraints for Table EPISODE
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."EPISODE" MODIFY ("IDEPISODE" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."EPISODE" MODIFY ("PATIENT_IDPATIENT" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."EPISODE" ADD CONSTRAINT "PK_EPISODE" PRIMARY KEY ("IDEPISODE")
  USING INDEX "C##ABHI"."PK_EPISODE"  ENABLE;
