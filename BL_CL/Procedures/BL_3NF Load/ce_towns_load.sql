-- PROCEDURE TO LOAD DATA TO CE_TOWNS
CREATE OR REPLACE PROCEDURE BL_CL.CE_TOWNS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_TOWNS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN

	WITH distinct_rows AS (
		SELECT DISTINCT COALESCE(UPPER(src.town), 'n. a.') AS town_name,
				src.province_id as province_src_id,
				CONCAT(src.province_id, '_', src.town_id) AS town_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
		UNION ALL
		SELECT DISTINCT COALESCE(UPPER(src.town), 'n. a.') AS town_name,
				src.province_id as province_src_id,
				CONCAT(src.province_id, '_', src.town_id) AS town_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
			FROM SA_SALES_ONLINE.src_sales_online AS src
		),
		insert_rows AS (
			SELECT  DISTINCT p.province_id AS province_id,
					map_t.town_name,
					map_t.town_id::VARCHAR AS town_src_id,
					'BL_CL' AS source_system,
					'T_MAP_TOWNS' AS source_entity
		FROM distinct_rows AS dr
		LEFT JOIN BL_CL.T_MAP_TOWNS AS map_t ON map_t.town_src_id = dr.town_src_id AND
												map_t.source_entity = dr.source_entity
		LEFT JOIN BL_CL.T_MAP_PROVINCES AS map_p ON map_p.province_src_id = dr.province_src_id AND 
													map_p.source_entity = dr.source_entity
		LEFT JOIN BL_3NF.CE_PROVINCES AS p ON p.province_src_id = map_p.province_id::VARCHAR AND
												p.source_entity = 'T_MAP_PROVINCES'
		)
		INSERT INTO BL_3NF.CE_TOWNS (town_id, province_id, town_name, town_src_id, source_system, source_entity) 
		SELECT nextval('BL_3NF.SEQ_CE_TOWN_ID') AS town_id,
				ir.province_id,
				ir.town_name,
				ir.town_src_id,
				ir.source_system,
				ir.source_entity
		FROM insert_rows AS ir
		WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_TOWNS AS curr WHERE ir.town_src_id = curr.town_src_id AND
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
