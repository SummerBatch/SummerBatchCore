﻿CREATE TABLE BATCH_JOB_INSTANCE  (
	JOB_INSTANCE_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT NULL,
	JOB_NAME VARCHAR(100) NOT NULL,
	JOB_KEY VARCHAR(32) NOT NULL,
	constraint JOB_INST_UN unique (JOB_NAME, JOB_KEY)
) ;

CREATE TABLE BATCH_JOB_EXECUTION  (
	JOB_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT NULL,
	JOB_INSTANCE_ID BIGINT NOT NULL,
	CREATE_TIME DATETIME NOT NULL,
	START_TIME DATETIME DEFAULT NULL ,
	END_TIME DATETIME DEFAULT NULL ,
	STATUS VARCHAR(10) NULL,
	EXIT_CODE VARCHAR(2500) NULL,
	EXIT_MESSAGE VARCHAR(2500) NULL,
	LAST_UPDATED DATETIME NULL,
	JOB_CONFIGURATION_LOCATION VARCHAR(2500) NULL,
	constraint JOB_INST_EXEC_FK foreign key (JOB_INSTANCE_ID)
	references BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
) ;

CREATE TABLE BATCH_JOB_EXECUTION_PARAMS  (
	JOB_EXECUTION_ID BIGINT NOT NULL ,
	TYPE_CD VARCHAR(6) NOT NULL ,
	KEY_NAME VARCHAR(100) NOT NULL ,
	STRING_VAL VARCHAR(250) NULL,
	DATE_VAL DATETIME DEFAULT NULL ,
	LONG_VAL BIGINT NULL,
	DOUBLE_VAL DOUBLE PRECISION NULL,
	IDENTIFYING CHAR(1) NOT NULL ,
	constraint JOB_EXEC_PARAMS_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ;

CREATE TABLE BATCH_STEP_EXECUTION  (
	STEP_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT NOT NULL,
	STEP_NAME VARCHAR(100) NOT NULL,
	JOB_EXECUTION_ID BIGINT NOT NULL,
	START_TIME DATETIME NOT NULL ,
	END_TIME DATETIME DEFAULT NULL ,
	STATUS VARCHAR(10) NULL,
	COMMIT_COUNT BIGINT NULL,
	READ_COUNT BIGINT NULL,
	FILTER_COUNT BIGINT NULL,
	WRITE_COUNT BIGINT NULL,
	READ_SKIP_COUNT BIGINT NULL,
	WRITE_SKIP_COUNT BIGINT NULL,
	PROCESS_SKIP_COUNT BIGINT NULL,
	ROLLBACK_COUNT BIGINT NULL,
	EXIT_CODE VARCHAR(2500) NULL,
	EXIT_MESSAGE VARCHAR(2500) NULL,
	LAST_UPDATED DATETIME NULL,
	constraint JOB_EXEC_STEP_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ;

CREATE TABLE BATCH_STEP_EXECUTION_CONTEXT  (
	STEP_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SERIALIZED_CONTEXT VARBINARY(MAX) NULL,
	constraint STEP_EXEC_CTX_FK foreign key (STEP_EXECUTION_ID)
	references BATCH_STEP_EXECUTION(STEP_EXECUTION_ID)
) ;

CREATE TABLE BATCH_JOB_EXECUTION_CONTEXT  (
	JOB_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SERIALIZED_CONTEXT VARBINARY(MAX) NULL,
	constraint JOB_EXEC_CTX_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ;

CREATE TABLE BATCH_STEP_EXECUTION_SEQ (ID BIGINT IDENTITY);
CREATE TABLE BATCH_JOB_EXECUTION_SEQ (ID BIGINT IDENTITY);
CREATE TABLE BATCH_JOB_SEQ (ID BIGINT IDENTITY);