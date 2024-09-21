-- Load CE_PROVINCES WITH FUNCTION. FUNCTION RETURNS ALL ROWS INSERTED TO THE CE_PROVINCES
CREATE OR REPLACE FUNCTION BL_CL.CE_PROVINCES_FUNC_LOAD()
RETURNS SETOF BL_3NF.CE_PROVINCES
LANGUAGE plpgsql
AS $$
DECLARE
	func_name TEXT = 'CE_PROVINCES_FUNC_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
	rec RECORD;
	query TEXT = 'INSERT INTO BL_3NF.CE_PROVINCES (province_id, province_name, province_src_id, source_system, source_entity) SELECT $1, $2, $3, $4, $5';
BEGIN
	FOR rec IN (
		SELECT new_row.province_name AS province_name,
				map_p.province_id::VARCHAR AS province_src_id,
				'BL_CL' AS source_system,
				'T_MAP_PROVINCES' AS source_entity
		FROM (
			SELECT DISTINCT COALESCE(UPPER(src.province), 'n. a') AS province_name,
					src.province_id AS province_src_id,
					'SA_SALES_OFFLINE' AS source_system,
					'SRC_SALES_OFFLINE' AS source_entity
			FROM SA_SALES_OFFLINE.src_sales_offline AS src
			UNION ALL
			SELECT DISTINCT COALESCE(UPPER(src.province), 'n. a') AS province_name,
					src.province_id AS province_src_id,
					'SA_SALES_ONLINE' AS source_system,
					'SRC_SALES_ONLINE' AS source_entity
			FROM SA_SALES_ONLINE.src_sales_online AS src
		) AS new_row 
		LEFT JOIN BL_CL.T_MAP_PROVINCES AS map_p ON map_p.province_src_id = new_row.province_src_id AND
													map_p.source_entity = new_row.source_entity	
		) LOOP 
			IF NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PROVINCES AS curr WHERE rec.province_src_id = curr.province_src_id AND
																			rec.source_entity = curr.source_entity) THEN 
																			
				INSERT INTO BL_3NF.CE_PROVINCES (province_id, province_name, province_src_id, source_system, source_entity)
				VALUES (nextval('SEQ_CE_PROVINCES'), rec.province_name, rec.province_src_id, rec.source_system, rec.source_entity);
				
				n_row := n_row + 1;
			
				RETURN NEXT rec;
			
			END IF;
		
		END LOOP;
	
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		n_row := ROW_COUNT;
	
	CALL BL_CL.log_procedure(func_name, n_row);

	RAISE NOTICE 'Data loaded sucessfully. % Rows inserted', n_row;
															
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Data insertion failed';
			
			GET STACKED DIAGNOSTICS 
				context := PG_EXCEPTION_CONTEXT,
				err_detail := PG_EXCEPTION_DETAIL;
			
			CALL BL_CL.log_procedure(func_name, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
			RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$;	
