-- PROCEDURE TO LOAD/REFRESH T MAP DISTRICTS DEDUPLICATION TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DISTRICTS_DEDUPLICATION()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DISTRICTS_DEDUPLICATION';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	INSERT INTO BL_CL.T_MAP_DISTRICTS (district_name, province_src_name, town_src_name, district_src_name, district_src_id, source_system, source_entity)
	SELECT new_row.district_name,
			new_row.province_src_name,
			new_row.town_src_name,
			new_row.district_src_name,
			new_row.district_src_id,
			new_row.source_system,
			new_row.source_entity
	FROM (
		SELECT DISTINCT UPPER(src.district) AS district_name,
				UPPER(src.province) AS province_src_name,
				UPPER(src.town) AS town_src_name,
				src.district AS district_src_name,
				src.town_id AS town_id,
				CONCAT(src.province_id, '_', src.town_id, '_', src.district_id) AS district_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.SRC_SALES_ONLINE AS src
	) AS new_row
	WHERE NOT EXISTS (SELECT 1 FROM BL_CL.T_MAP_DISTRICTS AS curr WHERE curr.district_name = new_row.district_name);
	
	INSERT INTO BL_CL.T_MAP_DISTRICTS (district_id, district_name, province_src_name, town_src_name, district_src_name, district_src_id, source_system, source_entity)
	SELECT	COALESCE(map_p.district_id, nextval('BL_CL.t_map_districts_id_seq')),
			new_row.district_name,
			new_row.province_src_name,
			new_row.town_src_name,
			new_row.district_src_name,
			new_row.district_src_id,
			new_row.source_system,
			new_row.source_entity
	FROM (
		SELECT DISTINCT UPPER(src.district) AS district_name,
				UPPER(src.province) AS province_src_name,
				UPPER(src.town) AS town_src_name,
				src.district AS district_src_name,
				src.town_id AS town_id,
				CONCAT(src.province_id, '_', src.town_id, '_', src.district_id) AS district_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE AS src
	) AS new_row
	LEFT JOIN BL_CL.T_MAP_DISTRICTS AS map_p ON map_p.district_name = new_row.district_name AND 
												map_p.province_src_name = new_row.province_src_name AND
												map_p.town_src_name = new_row.town_src_name
	WHERE NOT EXISTS (SELECT 1 FROM BL_CL.T_MAP_DISTRICTS AS curr WHERE curr.district_src_id = new_row.district_src_id AND
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