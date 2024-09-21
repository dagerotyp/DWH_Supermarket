-- PROCEDURE TO LOAD DATA TO DIM PAYMENTS TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DIM_DELIVERIES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DIM_DELIVERIES';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	-- Data load from BL_3NF Layer
	INSERT INTO BL_DM.DIM_DELIVERIES (delivery_surr_id, delivery_method_id, delivery_method_name, address_id, address, district_id, district_name,
									town_id, town_name, province_id, province_name, delivery_src_id, source_system, source_entity)
	SELECT nextval('BL_DM.SEQ_DIM_DELIVERY_SURR_ID') AS delivery_surr_id,
		d.delivery_method_id AS delivery_method_id,
		dm.delivery_method_name AS delivery_method_name,
		a.address_id AS address_id,
		a.address AS address,
		dis.district_id AS district_id,
		dis.district_name AS district_name,
		t.town_id AS town_id,
		t.town_name AS town_name,
		p.province_id AS province_id,
		p.province_name AS province_name,
		d.delivery_id::VARCHAR AS delivery_src_id,
		'BL_3NF' AS source_system,
		'CE_DELIVERIES' AS source_entity
	FROM BL_3NF.CE_DELIVERIES AS d
	LEFT JOIN BL_3NF.CE_DELIVERY_METHODS AS dm ON d.delivery_method_id = dm.delivery_method_id
	LEFT JOIN BL_3NF.CE_ADDRESSES AS a ON d.address_id = a.address_id
	LEFT JOIN BL_3NF.CE_DISTRICTS AS dis ON dis.district_id = a.district_id 
	LEFT JOIN BL_3NF.CE_TOWNS AS t ON dis.town_id = t.town_id
	LEFT JOIN BL_3NF.CE_PROVINCES AS p ON p.province_id = t.province_id 
	WHERE d.delivery_id <> -1 AND NOT EXISTS (SELECT 1 FROM BL_DM.DIM_DELIVERIES AS curr WHERE curr.delivery_src_id = d.delivery_id::VARCHAR AND
																	curr.source_entity = 'CE_DELIVERIES');
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
