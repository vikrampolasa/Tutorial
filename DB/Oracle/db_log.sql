/*
    UPGRADE_INFO_TBL        UPGRADE_INFO_TBL
    MI_DB_UPG_TRC_VALUES    UPGRADE_TRACE_VALUES    
    MI_DB_UPG_TRC_STEPS     UPGRADE_TRACE_STEPS
    MI_DB_UPG_EVENTS        UPGRADE_EVENTS_TBL
    MI_SYS_SEQ              UPGRADE_SEQUENCE
    MI_GLOBAL               UPGRADE_GLOBAL
*/

DECLARE
    v_obj_exists NUMBER; 
BEGIN 
    SELECT COUNT(*) 
        INTO v_obj_exists
    FROM user_tables 
    WHERE table_name = 'UPGRADE_INFO_TBL';
    
    IF v_obj_exists = 1 
    THEN 
        EXECUTE IMMEDIATE 'DROP TABLE UPGRADE_INFO_TBL';
    END IF; 
    
    SELECT COUNT(*) 
        INTO v_obj_exists
    FROM user_tables 
    WHERE table_name = 'UPGRADE_TRACE_VALUES';
    
    IF v_obj_exists = 1 
    THEN 
        EXECUTE IMMEDIATE 'DROP TABLE UPGRADE_TRACE_VALUES';
    END IF; 
    
    SELECT COUNT(*) 
        INTO v_obj_exists
    FROM user_tables 
    WHERE table_name = 'UPGRADE_TRACE_STEPS';
    
    IF v_obj_exists = 1 
    THEN 
        EXECUTE IMMEDIATE 'DROP TABLE UPGRADE_TRACE_STEPS';
    END IF; 
    
    SELECT COUNT(*) 
        INTO v_obj_exists
    FROM user_sequences 
    WHERE sequence_name = 'UPGRADE_SEQUENCE';
    
    IF v_obj_exists = 1 
    THEN 
        EXECUTE IMMEDIATE 'DROP SEQUENCE UPGRADE_SEQUENCE';
    END IF; 
    
    SELECT COUNT(*) 
        INTO v_obj_exists
    FROM user_tables 
    WHERE table_name = 'UPGRADE_EVENTS_TBL';
    
    IF v_obj_exists = 1 
    THEN 
        EXECUTE IMMEDIATE 'DROP TABLE UPGRADE_EVENTS_TBL';
    END IF; 
END;
/

--UPGRADE_INFO_TBL  upg_details_tbl
--dbdt_key updt_key
CREATE TABLE UPGRADE_INFO_TBL (dbdt_key NUMBER);

CREATE TABLE UPGRADE_TRACE_VALUES (
upg_trc_val_key NUMBER, 
step_key NUMBER, 
upg_trc_val_date TIMESTAMP, 
upg_trc_val_char VARCHAR2(4000), 
upg_trc_val_num FLOAT(126)
);
/

CREATE TABLE UPGRADE_TRACE_STEPS (
step_key NUMBER, 
dbdt_key NUMBER, 
step_proc_nm VARCHAR2(100),
step_nm VARCHAR2(4000), 
step_start_dt TIMESTAMP, 
step_msg_type_cd VARCHAR2(10), 
step_msg_cd VARCHAR2(4000)
);
/

CREATE TABLE UPGRADE_EVENTS_TBL (
event_xml XMLTYPE,
dbdt_key NUMBER
);
/

CREATE SEQUENCE UPGRADE_SEQUENCE START WITH 1;
/

CREATE OR REPLACE PACKAGE UPGRADE_GLOBAL AS 
trc_flg NUMBER; 
END UPGRADE_GLOBAL; 
/

CREATE OR REPLACE PACKAGE db_log AS 
	PROCEDURE create_event_xml (in_dbdt_key UPGRADE_INFO_TBL.dbdt_key%TYPE); 
    
    FUNCTION get_trc_flg RETURN NUMBER;
    
    PROCEDURE log_msg 
    (
        in_msg_type_cd IN UPGRADE_TRACE_STEPS.step_msg_type_cd%TYPE,
        in_dbdt_key IN UPGRADE_TRACE_STEPS.dbdt_key%TYPE,
        in_proc_nm IN UPGRADE_TRACE_STEPS.step_proc_nm%TYPE,
        in_step_nm IN UPGRADE_TRACE_STEPS.step_nm%TYPE,
        out_step_key IN OUT UPGRADE_TRACE_STEPS.step_key%TYPE
    );

    PROCEDURE log_error
    (
        in_dbdt_key IN UPGRADE_TRACE_STEPS.dbdt_key%TYPE,
        in_proc_nm IN UPGRADE_TRACE_STEPS.step_proc_nm%TYPE,
        in_step_nm IN UPGRADE_TRACE_STEPS.step_nm%TYPE,
        in_step_key IN UPGRADE_TRACE_STEPS.step_key%TYPE,
        in_err_number IN NUMBER,
        in_generic_err_msg IN CHAR VARYING,
        out_err_step_key IN OUT UPGRADE_TRACE_STEPS.step_key%TYPE,
        in_specific_err_msg IN CHAR VARYING
    );

    PROCEDURE persist_char_val 
    (
        in_msg_type_cd char varying, 
        in_step_key UPGRADE_TRACE_STEPS.step_key%type,
        in_parameter in varchar2
    );
    
    PROCEDURE persist_num_val 
    (
        in_msg_type_cd char varying, 
        in_step_key UPGRADE_TRACE_STEPS.step_key%type,
        in_parameter in float
    );
    
    PROCEDURE persist_date_val 
    (
        in_msg_type_cd char varying, 
        in_step_key UPGRADE_TRACE_STEPS.step_key%type,
        in_parameter in TIMESTAMP
    );
END;
/

CREATE OR REPLACE PACKAGE BODY db_log AS  

    PROCEDURE set_trc_flg (
    in_lvl IN NUMBER) 
    IS 
    BEGIN
        UPGRADE_GLOBAL.trc_flg := in_lvl; --check1: 
    END set_trc_flg; 
    
    FUNCTION get_trc_flg RETURN NUMBER
    IS 
    BEGIN
        RETURN 6 ; --UPGRADE_GLOBAL.trc_flg;
    END get_trc_flg; 
    
    PROCEDURE log_msg 
    (
        in_msg_type_cd IN UPGRADE_TRACE_STEPS.step_msg_type_cd%TYPE,
        in_dbdt_key IN UPGRADE_TRACE_STEPS.dbdt_key%TYPE,
        in_proc_nm IN UPGRADE_TRACE_STEPS.step_proc_nm%TYPE,
        in_step_nm IN UPGRADE_TRACE_STEPS.step_nm%TYPE,
        out_step_key IN OUT UPGRADE_TRACE_STEPS.step_key%TYPE
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        MSG_TYPE_CD UPGRADE_TRACE_STEPS.step_msg_type_cd%TYPE;
        msg_trace_level INT; 
    BEGIN
        IF in_dbdt_key IS NULL 
        THEN 
            RETURN;
        END IF; 
        
        dbms_application_info.set_module(in_proc_nm, in_step_nm);
        
        SELECT 
            CASE
                WHEN in_msg_type_cd = 'Fatal' THEN 1
                WHEN in_msg_type_cd = 'Error' THEN 2
                WHEN in_msg_type_cd = 'Warning' THEN 3
                WHEN in_msg_type_cd = 'Info' THEN 4
                WHEN in_msg_type_cd = 'Debug' THEN 5
            END
            INTO msg_trace_level
        FROM dual;
        
        --dbms_output.put_line(nvl(db_log.get_trc_flg, 9999)||' - '|| nvl(msg_trace_level, 999));
        
        IF db_log.get_trc_flg >= msg_trace_level THEN 
            SELECT UPGRADE_SEQUENCE.NEXTVAL INTO out_step_key FROM dual; 
            
            INSERT INTO UPGRADE_TRACE_STEPS
            (
                step_key, 
                dbdt_key,
                step_proc_nm, 
                step_nm, 
                step_start_dt,
                step_msg_type_cd
            )
            VALUES
            (
                out_step_key, 
                in_dbdt_key, 
                in_proc_nm, 
                in_step_nm,
                sysdate, --check2: mi_dbums_util.get_current_date
                in_msg_type_cd
            );
            COMMIT;
        END IF; 
    END log_msg;
    
    PROCEDURE persist_char_val 
    (
        in_msg_type_cd char varying, 
        in_step_key UPGRADE_TRACE_STEPS.step_key%type,
        in_parameter in varchar2
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION; 
        msg_trace_level INT; 
    BEGIN
        --Prevent unwanted data to be loaded
        IF in_step_key IS NULL
        THEN 
            RETURN;
        END IF; 
        
        SELECT 
            CASE
                WHEN in_msg_type_cd = 'Fatal' THEN 1
                WHEN in_msg_type_cd = 'Error' THEN 2
                WHEN in_msg_type_cd = 'Warning' THEN 3
                WHEN in_msg_type_cd = 'Info' THEN 4
                WHEN in_msg_type_cd = 'Debug' THEN 5
            END
            INTO msg_trace_level
        FROM dual;
        
        IF db_log.get_trc_flg >= msg_trace_level 
        THEN 
            INSERT INTO UPGRADE_TRACE_VALUES (upg_trc_val_key, step_key, upg_trc_val_char)
            VALUES (UPGRADE_SEQUENCE.NEXTVAL, in_step_key, SUBSTR(in_parameter, 1, 4000));
            COMMIT;
        END IF;
    END persist_char_val; 

    PROCEDURE persist_num_val 
    (
        in_msg_type_cd char varying, 
        in_step_key UPGRADE_TRACE_STEPS.step_key%type,
        in_parameter in float
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION; 
        msg_trace_level INT; 
    BEGIN
        --Prevent unwanted data to be loaded
        IF in_step_key IS NULL
        THEN 
            RETURN;
        END IF; 
        
        SELECT 
            CASE
                WHEN in_msg_type_cd = 'Fatal' THEN 1
                WHEN in_msg_type_cd = 'Error' THEN 2
                WHEN in_msg_type_cd = 'Warning' THEN 3
                WHEN in_msg_type_cd = 'Info' THEN 4
                WHEN in_msg_type_cd = 'Debug' THEN 5
            END
            INTO msg_trace_level
        FROM dual;
        
        IF db_log.get_trc_flg >= msg_trace_level 
        THEN 
            INSERT INTO UPGRADE_TRACE_VALUES (upg_trc_val_key, step_key, upg_trc_val_num)
            VALUES (UPGRADE_SEQUENCE.NEXTVAL, in_step_key, in_parameter);
            COMMIT;
        END IF;
    END persist_num_val; 

    PROCEDURE persist_date_val 
    (
        in_msg_type_cd char varying, 
        in_step_key UPGRADE_TRACE_STEPS.step_key%type,
        in_parameter in TIMESTAMP
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION; 
        msg_trace_level INT; 
    BEGIN
        --Prevent unwanted data to be loaded
        IF in_step_key IS NULL
        THEN 
            RETURN;
        END IF; 
        
        SELECT 
            CASE
                WHEN in_msg_type_cd = 'Fatal' THEN 1
                WHEN in_msg_type_cd = 'Error' THEN 2
                WHEN in_msg_type_cd = 'Warning' THEN 3
                WHEN in_msg_type_cd = 'Info' THEN 4
                WHEN in_msg_type_cd = 'Debug' THEN 5
            END
            INTO msg_trace_level
        FROM dual;
        
        IF db_log.get_trc_flg >= msg_trace_level 
        THEN 
            INSERT INTO UPGRADE_TRACE_VALUES (upg_trc_val_key, step_key, upg_trc_val_date)
            VALUES (UPGRADE_SEQUENCE.NEXTVAL, in_step_key, in_parameter);
            COMMIT;
        END IF;
    END persist_date_val; 
    
    PROCEDURE log_error
    (
        in_dbdt_key IN UPGRADE_TRACE_STEPS.dbdt_key%TYPE,
        in_proc_nm IN UPGRADE_TRACE_STEPS.step_proc_nm%TYPE,
        in_step_nm IN UPGRADE_TRACE_STEPS.step_nm%TYPE,
        in_step_key IN UPGRADE_TRACE_STEPS.step_key%TYPE,
        in_err_number IN NUMBER,
        in_generic_err_msg IN CHAR VARYING,
        out_err_step_key IN OUT UPGRADE_TRACE_STEPS.step_key%TYPE,
        in_specific_err_msg IN CHAR VARYING
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        
        msg_type_cd UPGRADE_TRACE_STEPS.step_msg_type_cd%TYPE := 'SqlError';
        msg_cd VARCHAR2(4000);
        db_name VARCHAR2(1000);
    BEGIN
        IF in_dbdt_key IS NULL 
        THEN 
            RETURN;
        END IF; 
        
        dbms_application_info.set_module(in_proc_nm, in_step_nm);
        
        msg_cd := in_proc_nm || '_' || in_step_nm || '_SQL_' || in_err_number;
        SELECT user INTO db_name FROM user_users;
        
        IF db_log.get_trc_flg > 2 
        THEN
            BEGIN 
                UPDATE UPGRADE_TRACE_STEPS 
                SET step_msg_type_cd = msg_type_cd,
                    step_msg_cd = msg_cd
                WHERE step_key = in_step_key;
                
                out_err_step_key := in_step_key; 
                
            EXCEPTION
                WHEN OTHERS THEN 
                SELECT UPGRADE_SEQUENCE.NEXTVAL INTO out_err_step_key FROM dual; 
                
                INSERT INTO UPGRADE_TRACE_STEPS
                (
                    step_key, 
                    dbdt_key,
                    step_proc_nm, 
                    step_nm, 
                    step_start_dt,
                    step_msg_type_cd,
                    step_msg_cd
                )
                VALUES
                (
                    out_err_step_key, 
                    in_dbdt_key, 
                    in_proc_nm, 
                    in_step_nm,
                    sys_extract_utc(systimestamp), --check2: mi_dbums_util.get_current_date
                    msg_type_cd,
                    msg_cd
                ); 
            END;
        END IF; 
        
        db_log.persist_num_val('Info', out_err_step_key, in_err_number);
        db_log.persist_char_val('Info', out_err_step_key, in_generic_err_msg||in_specific_err_msg);
        commit;
    END log_error; 
    
	PROCEDURE create_event_xml (in_dbdt_key UPGRADE_INFO_TBL.dbdt_key%TYPE) AS 
	
			CURSOR event_xml_csr IS 
			SELECT XMLELEMENT("A", XMLAGG(XMLELEMENT("EventId", 
								XMLAGG(XMLFOREST(step_msg_type_cd "Name", 
												step.step_proc_nm "ProcNm", 
												step.step_nm "StepNm", 
												TO_CHAR(step.step_start_dt, 'DD-MON-YY HH:MI:SSPM') step_start_dt, 
												step.step_msg_cd "ErrorMessage")),
					XMLELEMENT ("EventParameters", 
							(SELECT XMLAGG (XMLFOREST(COALESCE(CAST(TO_CHAR(dbtv.upg_trc_val_date, 'DD-MON-YY HH:MIPM') AS VARCHAR2(100)),
													dbtv.upg_trc_val_char, 
													CAST(dbtv.upg_trc_val_num AS VARCHAR2(100))
									) AS "EventParameter"))
				FROM UPGRADE_TRACE_VALUES dbtv
				WHERE step.step_key = dbtv.step_key
				))) ORDER BY step.step_key ASC)).EXTRACT ('/A/*') AS EventId
			FROM UPGRADE_TRACE_STEPS step
			WHERE step.dbdt_key = in_dbdt_key 
			GROUP BY step.step_key 
			ORDER BY step.step_key;

			CURSOR event_xml_csr_old IS 
			SELECT XMLELEMENT("EventId", 
								XMLAGG(XMLFOREST(step_msg_type_cd "Name", 
												step.step_proc_nm "ProcNm", 
												step.step_nm "StepNm", 
												TO_CHAR(step.step_start_dt, 'DD-MON-YY HH:MI:SSPM') step_start_dt, 
												step.step_msg_cd "ErrorMessage")),
					XMLELEMENT ("EventParameters", 
							(SELECT XMLAGG (XMLFOREST(COALESCE(CAST(TO_CHAR(dbtv.upg_trc_val_date, 'DD-MON-YY HH:MIPM') AS VARCHAR2(100)),
													dbtv.upg_trc_val_char, 
													CAST(dbtv.upg_trc_val_num AS VARCHAR2(100))
									) AS "EventParameter"))
				FROM UPGRADE_TRACE_VALUES dbtv
				WHERE step.step_key = dbtv.step_key
				))).EXTRACT ('/A/*') AS EventId
			FROM UPGRADE_TRACE_STEPS step
			WHERE step.dbdt_key = in_dbdt_key 
			GROUP BY step.step_key 
			ORDER BY step.step_key;
            
            d_step_key UPGRADE_TRACE_STEPS.step_key%TYPE;
	BEGIN 
		IF db_log.get_trc_flg < 5
		THEN
			RETURN;
		END IF;
	
		FOR event_xml_rec IN event_xml_csr LOOP
			UPDATE UPGRADE_EVENTS_TBL 
			SET event_xml = insertchildxml(event_xml, 'A', 'EventId', event_xml_rec.EventId)
			WHERE dbdt_key = in_dbdt_key;
		
			IF SQL%notfound THEN
				INSERT INTO UPGRADE_EVENTS_TBL (dbdt_key, event_xml)
				VALUES (in_dbdt_key, event_xml_rec.eventid);
			END IF;
		END LOOP;
	EXCEPTION
    WHEN OTHERS THEN
		db_log.log_error (in_dbdt_key, 'LOG_CREATE_EVENT_XML', 'UPDATING UPGRADE_EVENTS_TBL', d_step_key, SQLCODE, SQLERRM, d_step_key, '');
		db_log.log_msg('Info', in_dbdt_key, 'LOG_CREATE_EVENT_XML', 'ERROR_WHILE_UPDATING UPGRADE_EVENTS_TBL', d_step_key);
	
		FOR event_xml_rec IN event_xml_csr_old LOOP
			UPDATE UPGRADE_EVENTS_TBL
			SET event_xml = insertchildxml (event_xml, 'A', 'EventId', event_xml_rec.EventId)
			WHERE dbdt_key = in_dbdt_key;
		
			IF SQL%notfound THEN
				INSERT INTO UPGRADE_EVENTS_TBL (dbdt_key, event_xml)
				VALUES (in_dbdt_key, event_xml_rec.EventId);
			END IF;			
		END LOOP;	
	END create_event_xml;	
END; 
/

 set serveroutput on;
 declare
    proc_nm VARCHAR2(100) := 'TEST_PROC';
    step_nm VARCHAR2(100);
    d_dbdt_key NUMBER := 10;
    d_step_key NUMBER;
    d_msg VARCHAR2(1000);
 begin
    delete from UPGRADE_TRACE_STEPS;
    delete from UPGRADE_TRACE_VALUES; 
    
    step_nm := 'Initial step';
    db_log.log_msg('Info', d_dbdt_key, proc_nm, step_nm, d_step_key); 
    
    dbms_session.sleep(5); 
    
    step_nm := 'Second step';
    db_log.log_msg('Info', d_dbdt_key, proc_nm, step_nm, d_step_key); 
    
    d_msg := 'Data in values table';
    db_log.persist_char_val('Info', d_step_key, d_msg);
end; 
/

--select * from UPGRADE_TRACE_STEPS;
--select * from UPGRADE_TRACE_VALUES; 
