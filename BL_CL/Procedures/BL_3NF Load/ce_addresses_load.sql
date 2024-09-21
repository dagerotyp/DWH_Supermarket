-- PROCEDURE TO LOAD DATA TO CE_ADDRESSES
CREATE OR REPLACE PROCEDURE BL_CL.CE_ADDRESSES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_ADDRESSES_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	
	WITH distinct_row AS (
		SELECT DISTINCT COALESCE(UPPER(src.delivery_address), 'n. a.') AS address,
				CONCAT(src.province_id, '_', src.town_id, '_', src.district_id) AS district_src_id,
				CONCAT(src.province_id, '_', src.town_id, '_', src.district_id, '_', src.delivery_address) AS address_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src
		UNION ALL
		SELECT DISTINCT COALESCE(UPPER(src.shop_address), 'n. a.') AS address,
				CONCAT(src.province_id, '_', src.town_id, '_', src.district_id) AS district_src_id,
				src.shop_name AS address_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
	),
	insert_row AS (
		SELECT dr.address,
				d.district_id,
				dr.address_src_id,
				dr.source_system,
				dr.source_entity
		FROM distinct_row AS dr
		LEFT JOIN BL_CL.T_MAP_DISTRICTS AS map_d ON map_d.district_src_id = dr.district_src_id AND
													map_d.source_entity = dr.source_entity
		LEFT JOIN BL_3NF.CE_DISTRICTS AS d ON d.district_src_id = map_d.district_id::VARCHAR AND 
												d.source_entity = 'T_MAP_DISTRICTS'
	)
	INSERT INTO BL_3NF.CE_ADDRESSES (address_id, district_id, address, address_src_id, source_system, source_entity) 
	SELECT nextval('BL_3NF.SEQ_CE_ADDRESS_ID') AS address_id,
			ir.district_id,
			ir.address,
			ir.address_src_id,
			ir.source_system,
			ir.source_entity
	FROM insert_row AS ir
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_ADDRESSES AS curr WHERE ir.address_src_id = curr.address_src_id AND
																	ir.source_system = curr.source_system);
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
