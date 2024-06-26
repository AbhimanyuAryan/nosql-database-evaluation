--------------------------------------------------------
--  DDL for Table LAB_SCREENING
--------------------------------------------------------

  CREATE TABLE "C##ABHI"."LAB_SCREENING" 
   (	"LAB_ID" NUMBER(*,0) DEFAULT "C##ABHI"."SEQ_LAB_ID"."NEXTVAL", 
	"TEST_COST" NUMBER(10,2), 
	"TEST_DATE" DATE, 
	"IDTECHNICIAN" NUMBER(*,0), 
	"EPISODE_IDEPISODE" NUMBER(*,0)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS" ;
