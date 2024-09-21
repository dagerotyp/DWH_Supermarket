-- PROCEDURE TO LOAD DATA TO CE_STORES
CREATE OR REPLACE PROCEDURE BL_CL.CE_STORES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_STORES_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN

	WITH distinct_row AS (
		SELECT distinct COALESCE(UPPER(src.shop_name), 'n. a.') AS store_name,
				'n. a.' AS website,
				'n. a.' AS email,
				src.shop_name AS store_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src 
		UNION ALL
		SELECT distinct 'n. a.' AS store_name,
				COALESCE(UPPER(src.shop_online_address), 'n. a.') AS website,
				COALESCE(UPPER(src.shop_email), 'n. a.') AS email,
				src.shop_online_address AS store_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src 
	),
	ce_row AS (
		SELECT dr.store_name,
				dr.website,
				dr.email,
				COALESCE(a.address_id, -1) AS address_id,
				dr.store_src_id,
				dr.source_system,
				dr.source_entity
		FROM distinct_row AS dr
		LEFT JOIN BL_3NF.CE_ADDRESSES AS a ON a.address_src_id = dr.store_name AND 
												a.source_entity = dr.source_entity
	)
	INSERT INTO BL_3NF.CE_STORES (store_id, website, address_id, store_name, email, store_src_id, source_system, source_entity)
	SELECT  nextval('BL_3NF.SEQ_CE_STORE_ID') AS store_id,
			ir.website,
			ir.address_id,
			ir.store_name,
			ir.email,
			ir.store_src_id,
			ir.source_system,
			ir.source_entity
	FROM ce_row AS ir
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.ce_stores AS curr WHERE ir.store_src_id = curr.store_src_id AND
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
