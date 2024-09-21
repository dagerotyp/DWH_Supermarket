-- PROCEDURE TO LOAD DATA TO CE_DELIVERY_METHODS
CREATE OR REPLACE PROCEDURE BL_CL.CE_DELIVERY_METHODS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_DELIVERY_METHODS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN																										
												
	INSERT INTO BL_3NF.CE_DELIVERY_METHODS (delivery_method_id, delivery_method_name, delivery_method_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_DELIVERY_METHOD_ID') AS delivery_method_id,
			new_row.delivery_method_name,
			new_row.delivery_method_src_id,
			new_row.source_system,
			new_row.source_entity
	FROM (
		SELECT distinct COALESCE(UPPER(src.delivery_method), 'n. a.') AS delivery_method_name,
				src.delivery_method AS delivery_method_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src 
	) AS new_row
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.ce_delivery_methods AS curr WHERE new_row.delivery_method_src_id = curr.delivery_method_src_id AND
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
