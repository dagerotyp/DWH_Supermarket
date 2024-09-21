-- PROCEDURE TO LOAD DATA TO CE_PAYMENT_METHODS
CREATE OR REPLACE PROCEDURE BL_CL.CE_PAYMENT_METHODS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_PAYMENT_METHODS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN

	INSERT INTO BL_3NF.CE_PAYMENT_METHODS (payment_method_id, payment_name, payment_method_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_PAYMENT_METHOD_ID') AS payment_method_id,
		new_row.payment_name,
		new_row.payment_method_src_id,
		new_row.source_system,
		new_row.source_entity
	FROM (
		SELECT distinct COALESCE(UPPER(src.payment_method), 'n. a.') AS payment_name,
				src.payment_method AS payment_method_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src 
		UNION ALL
		SELECT distinct COALESCE(UPPER(src.payment_method), 'n. a.') AS payment_name,
				src.payment_method AS payment_method_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src 
	) AS new_row
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.ce_payment_methods AS curr WHERE new_row.payment_method_src_id = curr.payment_method_src_id AND
																	new_row.source_entity = curr.source_entity);
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
