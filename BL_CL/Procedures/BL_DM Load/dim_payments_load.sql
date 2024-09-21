-- PROCEDURE TO LOAD DATA TO DIM PAYMENTS TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DIM_PAYMENTS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DIM_PAYMENTS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	INSERT INTO BL_DM.DIM_PAYMENTS (payment_surr_id, payment_name, payment_src_id, source_system, source_entity)
	SELECT nextval('BL_DM.SEQ_DIM_PAYMENT_SURR_ID') payment_surr_id,
			p.payment_name AS payment_name,
			p.payment_method_id::VARCHAR AS payment_src_id,
			'BL_3NF' AS source_system,
			'CE_PAYMENT_METHODS' AS source_entity
	FROM BL_3NF.CE_PAYMENT_METHODS AS p
	WHERE p.payment_method_id <> -1 AND NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PAYMENTS AS curr WHERE curr.payment_src_id = p.payment_method_id::VARCHAR AND
																	curr.source_entity = 'CE_PAYMENT_METHODS');
	
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
