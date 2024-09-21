-- PROCEDURE TO LOAD DATA TO DIM STORES TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DIM_STORES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DIM_STORES';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	INSERT INTO BL_DM.DIM_STORES (store_surr_id, address_id, address, district_id, district_name, town_id, town_name, province_id, province_name, store_name,
									email, website, store_src_id, source_system, source_entity)		
	SELECT nextval('BL_DM.SEQ_DIM_STORE_SURR_ID'),
			a.address_id,
			a.address,
			dis.district_id,
			dis.district_name,
			t.town_id,
			t.town_name,
			p.province_id,
			p.province_name,
			s.store_name,
			s.email,
			s.website,
			s.store_id::VARCHAR AS store_src_id,
			'BL_3NF' AS source_system,
			'CE_STORES' AS source_entity
	FROM BL_3NF.CE_STORES AS s
	LEFT JOIN BL_3NF.CE_ADDRESSES AS a ON s.address_id = a.address_id
	LEFT JOIN BL_3NF.CE_DISTRICTS AS dis ON dis.district_id = a.district_id 
	LEFT JOIN BL_3NF.CE_TOWNS AS t ON dis.town_id = t.town_id
	LEFT JOIN BL_3NF.CE_PROVINCES AS p ON p.province_id = t.province_id 
	WHERE s.store_id <> -1 AND NOT EXISTS (SELECT 1 FROM BL_DM.DIM_STORES AS curr WHERE curr.store_src_id = s.store_id::VARCHAR AND
																	curr.source_entity = 'CE_STORES');
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
