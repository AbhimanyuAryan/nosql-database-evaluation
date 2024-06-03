--------------------------------------------------------
--  Constraints for Table ROOM
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."ROOM" MODIFY ("IDROOM" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."ROOM" ADD CONSTRAINT "PK_ROOM" PRIMARY KEY ("IDROOM")
  USING INDEX "C##ABHI"."PK_ROOM"  ENABLE;
