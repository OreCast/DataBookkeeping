--------------------------------------------------------
--  DDL for Table DATASETS
--------------------------------------------------------

  CREATE TABLE "DATASETS" 
   (	"DATASET_ID" INTEGER, 
	"DATASET" VARCHAR2(700), 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500)
   ) ;
--------------------------------------------------------
--  DDL for Table FILES
--------------------------------------------------------

  CREATE TABLE "FILES" 
   (	"FILE_ID" INTEGER, 
	"LOGICAL_FILE_NAME" VARCHAR2(500), 
	"IS_FILE_VALID" INTEGER DEFAULT 1, 
	"DATASET_ID" INTEGER, 
	"CHECK_SUM" VARCHAR2(100), 
	"EVENT_COUNT" INTEGER, 
	"FILE_SIZE" INTEGER, 
	"BRANCH_HASH_ID" INTEGER, 
	"ADLER32" VARCHAR2(100) DEFAULT NULL, 
	"MD5" VARCHAR2(100) DEFAULT NULL, 
	"CREATION_DATE" INTEGER, 
	"CREATE_BY" VARCHAR2(500), 
	"LAST_MODIFICATION_DATE" INTEGER, 
	"LAST_MODIFIED_BY" VARCHAR2(500)
   ) ;
