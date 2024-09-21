-- PROCEDURE TO LOAD/REFRESH T MAP PROVINCES DEDUPLICATION TABLE
CREATE OR REPLACE PROCEDURE BL_CL.PROVINCES_DEDUPLICATION()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.PROVINCES_DEDUPLICATION';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	INSERT INTO BL_CL.T_MAP_PROVINCES (province_name, province_src_name, province_src_id, source_system, source_entity)
	SELECT new_row.province_name,
			new_row.province_src_name,
			new_row.province_src_id,
			new_row.source_system,
			new_row.source_entity
	FROM (
		SELECT DISTINCT UPPER(src.province) AS province_name,
				src.province AS province_src_name,
				src.province_id AS province_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.SRC_SALES_ONLINE AS src
	) AS new_row
	WHERE NOT EXISTS (SELECT 1 FROM BL_CL.T_MAP_PROVINCES AS curr WHERE curr.province_name = new_row.province_name);

	INSERT INTO BL_CL.T_MAP_PROVINCES (province_id, province_name, province_src_name, province_src_id, source_system, source_entity)
	SELECT	COALESCE(map_p.province_id, nextval('BL_CL.t_map_provinces_id_seq')),
			new_row.province_name,
			new_row.province_source_name,
			new_row.province_src_id,
			new_row.source_system,
			new_row.source_entity
	FROM (
		SELECT DISTINCT UPPER(src.province) AS province_name,
				src.province AS province_source_name,
				src.province_id AS province_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE AS src)
	AS new_row
	LEFT JOIN BL_CL.T_MAP_PROVINCES AS map_p ON map_p.province_name = new_row.province_name
	WHERE NOT EXISTS (SELECT 1 FROM BL_CL.T_MAP_PROVINCES AS curr WHERE curr.province_src_id = new_row.province_src_id AND
																		curr.source_entity = new_row.source_entity);
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