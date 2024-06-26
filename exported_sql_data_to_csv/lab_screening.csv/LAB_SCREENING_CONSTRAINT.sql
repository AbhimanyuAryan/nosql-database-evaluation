--------------------------------------------------------
--  Constraints for Table LAB_SCREENING
--------------------------------------------------------

  ALTER TABLE "C##ABHI"."LAB_SCREENING" MODIFY ("LAB_ID" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."LAB_SCREENING" MODIFY ("IDTECHNICIAN" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."LAB_SCREENING" MODIFY ("EPISODE_IDEPISODE" NOT NULL ENABLE);
  ALTER TABLE "C##ABHI"."LAB_SCREENING" ADD CONSTRAINT "LAB_SCREENING_PK" PRIMARY KEY ("LAB_ID")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS"  ENABLE;
