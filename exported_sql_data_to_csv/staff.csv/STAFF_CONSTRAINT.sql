--------------------------------------------------------
--  Constraints for Table STAFF
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("EMP_ID" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("EMP_FNAME" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("EMP_LNAME" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("ADDRESS" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("SSN" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("IDDEPARTMENT" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" MODIFY ("IS_ACTIVE_STATUS" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."STAFF" ADD CONSTRAINT "CHECK_STAFF_STATUS" CHECK (is_active_status IN ('Y', 'N')) ENABLE;
  ALTER TABLE "C##ABHI"."STAFF" ADD CONSTRAINT "PK_STAFF" PRIMARY KEY ("EMP_ID")
  USING INDEX "C##ABHI"."PK_STAFF"  ENABLE;
