-- PROCEDURE TO LOAD DATA TO DIM CUSTOMERS TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DIM_CUSTOMERS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DIM_CUSTOMERS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	INSERT INTO BL_DM.DIM_CUSTOMERS (customer_surr_id, first_name, last_name, email, phone_number, gender, birthdate_dt, customer_src_id, source_system, source_entity)
	SELECT nextval('BL_DM.SEQ_DIM_CUSTOMER_SURR_ID') AS customer_surr_id,
			c.first_name AS first_name,
			c.last_name AS last_name,
			c.email AS email,
			c.phone_number AS phone_number,
			c.gender AS gender,
			c.birthdate_dt AS birthdate_dt,
			c.customer_id::VARCHAR AS customer_src_id,
			'BL_3NF' AS source_system,
			'CE_CUSTOMERS' AS source_entity
	FROM BL_3NF.CE_CUSTOMERS AS c
	WHERE c.customer_id <> -1 AND NOT EXISTS (SELECT 1 FROM BL_DM.DIM_CUSTOMERS AS curr WHERE curr.customer_src_id = c.customer_id::VARCHAR AND
																	curr.source_entity = 'CE_CUSTOMERS');
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
