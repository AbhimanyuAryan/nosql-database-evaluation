--------------------------------------------------------
--  Constraints for Table INSURANCE
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."INSURANCE" ADD CONSTRAINT "PK_INSURANCE" PRIMARY KEY ("POLICY_NUMBER")
  USING INDEX "C##ABHI"."PK_INSURANCE"  ENABLE;
