-- PROCEDURE TO LOAD DATA TO CE_EMPLOYEES_SCD
CREATE OR REPLACE PROCEDURE BL_CL.CE_EMPLOYEES_SCD_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_EMPLOYEES_SCD_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	MERGE INTO BL_3NF.CE_EMPLOYEES_SCD target USING (
	WITH src AS (
		SELECT distinct COALESCE(UPPER(src.emp_email), 'n. a') AS email,
				COALESCE(UPPER(src.emp_first_name), 'n. a') AS first_name,
				COALESCE(UPPER(src.emp_last_name), 'n. a') AS last_name,	
				COALESCE(src.emp_phone_number, 'n. a') AS phone_number,
				COALESCE(UPPER(src.emp_gender), 'n. a') AS gender,
				src.emp_id AS employee_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src)
	SELECT src.employee_src_id AS match_key,
			NULL AS curr_id,
			src.*
	FROM src
	UNION
	SELECT NULL AS match_key,
			emp_scd.employee_id AS curr_id,
			src.*
	FROM src
	LEFT JOIN BL_3NF.CE_EMPLOYEES_SCD AS emp_scd ON emp_scd.employee_src_id = src.employee_src_id
	WHERE emp_scd.employee_src_id IS NOT NULL AND 
			NOT EXISTS (SELECT 1 FROM BL_3NF.CE_EMPLOYEES_SCD AS emp_scd WHERE src.email = emp_scd.email AND 
																			src.first_name = emp_scd.first_name AND 
																			src.last_name = emp_scd.last_name AND 
																			src.phone_number = emp_scd.phone_number AND
																			src.gender = emp_scd.gender)
	EXCEPT 
	SELECT emp_scd.employee_src_id AS match_key,
			NULL AS curr_id,
			emp_scd.email, emp_scd.first_name, emp_scd.last_name, emp_scd.phone_number, emp_scd.gender, emp_scd.employee_src_id, emp_scd.source_system, emp_scd.source_entity
	FROM BL_3NF.CE_EMPLOYEES_SCD AS emp_scd
	) AS src
	ON src.match_key = target.employee_src_id AND target.is_active = TRUE
	WHEN MATCHED AND (src.email <> target.email OR src.first_name <> target.first_name OR src.last_name <> target.last_name OR src.phone_number <> target.phone_number OR src.gender <> target.gender)
	THEN UPDATE 
		SET end_dt = current_timestamp,
			is_active = FALSE
	WHEN NOT MATCHED 
	THEN INSERT (employee_id, email, first_name, last_name, phone_number, gender, employee_src_id, source_system, source_entity)
	VALUES  (COALESCE (src.curr_id, nextval('BL_3NF.SEQ_CE_EMPLOYEE_ID')),
			src.email,
			src.first_name,
			src.last_name,
			src.phone_number,
			src.gender,
			src.employee_src_id,
			src.source_system,
			src.source_entity);
-- LOGGING
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		n_row := ROW_COUNT;
	
	CALL BL_CL.log_procedure(proc_name, n_row);

	RAISE NOTICE 'Data loaded sucessfully. % Rows inserted', n_row;
																
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;
			RAISE NOTICE 'Data insertion failed';
				
			GET STACKED DIAGNOSTICS 
				context := PG_EXCEPTION_CONTEXT,
				err_detail := PG_EXCEPTION_DETAIL;
				
			CALL BL_CL.log_procedure(proc_name, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
			RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$;
