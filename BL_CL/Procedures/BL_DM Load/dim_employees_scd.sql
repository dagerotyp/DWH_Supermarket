-- PROCEDURE TO LOAD DATA TO DIM EMPLOYEES SCD TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DIM_EMPLOYEES_SCD_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DIM_EMPLOYEES_SCD_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	MERGE INTO BL_DM.DIM_EMPLOYEES_SCD AS target USING (
		WITH src AS (
			SELECT e.employee_id::VARCHAR AS employee_src_id,
					e.first_name AS first_name,
					e.last_name AS last_name,
					e.email AS email,
					e.phone_number AS phone_number,
					e.gender AS gender,
					e.start_dt AS start_dt,
					e.end_dt AS end_dt,
					e.is_active AS is_active,	
					'BL_3NF' AS source_system,
					'CE_EMPLOYEES_SCD' AS source_entity
			FROM BL_3NF.CE_EMPLOYEES_SCD AS e
			WHERE e.employee_id != -1
			EXCEPT 
			SELECT curr.employee_src_id, curr.first_name, curr.last_name, curr.email, curr.phone_number, curr.gender, curr.start_dt, curr.end_dt, curr.is_active, curr.source_system, curr.source_entity
			FROM BL_DM.DIM_EMPLOYEES_SCD AS curr
		)
		SELECT s.employee_src_id AS match_key,
				NULL AS curr_id,
				s.first_name AS first_name,
				s.last_name AS last_name,
				s.email AS email,
				s.phone_number AS phone_number,
				s.gender AS gender,
				s.start_dt AS start_dt,
				s.end_dt AS end_dt,
				s.is_active AS is_active,
				s.employee_src_id AS employee_src_id,
				s.source_system AS source_system,
				s.source_entity AS source_entity
		FROM src AS s
		WHERE s.is_active = FALSE
		UNION 
		SELECT NULL AS match_key,
				emp_scd.employee_surr_id AS curr_id,
				s.first_name AS first_name,
				s.last_name AS last_name,
				s.email AS email,
				s.phone_number AS phone_number,
				s.gender AS gender,
				s.start_dt AS start_dt,
				s.end_dt AS end_dt,
				s.is_active AS is_active,
				s.employee_src_id AS employee_src_id,
				s.source_system AS source_system,
				s.source_entity AS source_entity
		FROM src AS s
		LEFT JOIN BL_DM.DIM_EMPLOYEES_SCD AS emp_scd ON emp_scd.employee_src_id = s.employee_src_id
		WHERE s.is_active = TRUE
	) AS src
	ON src.match_key = target.employee_src_id
	WHEN MATCHED
	THEN UPDATE 
		SET end_dt = src.end_dt,
			is_active = src.is_active
	WHEN NOT MATCHED 
	THEN INSERT (employee_surr_id, first_name, last_name, email, phone_number, gender, start_dt, end_dt, is_active, employee_src_id, source_system, source_entity)
		VALUES (COALESCE(src.curr_id,nextval('BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID')), src.first_name, src.last_name, src.email, src.phone_number, src.gender, src.start_dt, src.end_dt, src.is_active, src.employee_src_id, src.source_system, src.source_entity);

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
