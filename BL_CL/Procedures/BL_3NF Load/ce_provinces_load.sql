-- PROCEDURE TO LOAD DATA TO CE_PROVINCES
CREATE OR REPLACE PROCEDURE BL_CL.CE_PROVINCES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_PROVINCES_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	WITH distinct_rows AS (
		SELECT DISTINCT COALESCE(UPPER(src.province), 'n. a') AS province_name,
				src.province_id AS province_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
		UNION
		SELECT DISTINCT COALESCE(UPPER(src.province), 'n. a') AS province_name,
				src.province_id AS province_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src
	),
	insert_rows AS (
		SELECT DISTINCT dr.province_name AS province_name,
					map_p.province_id::VARCHAR AS province_src_id,
					'BL_CL' AS source_system,
					'T_MAP_PROVINCES' AS source_entity
		FROM distinct_rows AS dr
		LEFT JOIN BL_CL.T_MAP_PROVINCES AS map_p ON map_p.province_src_id = dr.province_src_id
	)
	INSERT INTO BL_3NF.CE_PROVINCES (province_id, province_name, province_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_PROVINCES_ID') AS province_id,
			ir.province_name AS province_name,
			ir.province_src_id AS province_src_id,
			ir.source_system AS source_system,
			ir.source_entity AS source_entity
	FROM insert_rows AS ir
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PROVINCES AS curr WHERE ir.province_src_id = curr.province_src_id AND
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
