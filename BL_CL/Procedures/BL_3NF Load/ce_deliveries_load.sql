-- PROCEDURE TO LOAD DATA TO CE_DELIVERIES
CREATE OR REPLACE PROCEDURE BL_CL.CE_DELIVERIES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_DELIVERIES_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	-- Data load from src_sales_online													
	WITH distinct_row AS (
		SELECT distinct src.order_id AS delivery_src_id,
				src.province_id AS province_id,
				src.town_id AS town_id,
				src.district_id AS district_id,
				src.delivery_address as delivery_address,
				src.delivery_method as delivery_method,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src
	),
	ce_row AS (
		SELECT 	dm.delivery_method_id,
				a.address_id,
				dr.delivery_src_id,
				dr.source_system,
				dr.source_entity
		FROM distinct_row AS dr
		LEFT JOIN BL_3NF.CE_DELIVERY_METHODS AS dm ON dm.delivery_method_src_id = dr.delivery_method AND 
														dm.source_entity = dr.source_entity
		LEFT JOIN BL_3NF.CE_ADDRESSES AS a ON a.address_src_id = CONCAT(dr.province_id, '_' ,dr.town_id, '_', dr.district_id, '_', dr.delivery_address) AND 
												a.source_entity = dr.source_entity
	)
	INSERT INTO BL_3NF.CE_DELIVERIES (delivery_id, delivery_method_id, address_id, delivery_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_DELIVERY_ID') AS delivery_id,
			ir.delivery_method_id,
			ir.address_id,
			ir.delivery_src_id,
			ir.source_system,
			ir.source_entity
	FROM ce_row AS ir
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.ce_deliveries AS curr WHERE ir.delivery_src_id = curr.delivery_src_id AND
																	ir.source_entity = curr.source_entity);
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
