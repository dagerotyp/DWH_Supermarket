-- PROCEDURE TO LOAD DATA TO CE_CUSTOMERS
CREATE OR REPLACE PROCEDURE BL_CL.CE_CUSTOMERS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_CUSTOMERS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN

	INSERT INTO BL_3NF.CE_CUSTOMERS (customer_id,  first_name, last_name, email, phone_number, gender, birthdate_dt, customer_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_CUSTOMER_ID') AS customer_id,
			new_row.first_name,
			new_row.last_name,
			new_row.email,
			new_row.phone_number,
			new_row.gender,
			new_row.birthdate_dt,
			new_row.customer_src_id,
			new_row.source_system,
			new_row.source_entity
	FROM (
		SELECT DISTINCT src.client_id AS customer_src_id,
				'n. a.' AS email,
				COALESCE(UPPER(SPLIT_PART(src.fullname, ' ', 1)), 'n. a.') AS first_name,
				COALESCE(UPPER(SPLIT_PART(src.fullname, ' ', 2)), 'n. a.') AS last_name,
				COALESCE(UPPER(src.phone_number), 'n. a.') AS phone_number,
				COALESCE(UPPER(src.gender), 'n. a.') AS gender,
				'1990-01-01'::DATE AS birthdate_dt,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
		UNION ALL
		SELECT distinct src.client_id AS customer_src_id,
				COALESCE(UPPER(src.email), 'n. a.') AS email,
				COALESCE(UPPER(src.first_name), 'n. a.') AS first_name,
				COALESCE(UPPER(src.last_name), 'n. a.') AS last_name,
				'n. a.' AS phone_number,
				COALESCE(UPPER(src.gender), 'n. a.') AS gender,
				COALESCE(src.client_birthdate::DATE, '1990-01-01'::DATE) AS birthdate_dt,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src
	) AS new_row
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_CUSTOMERS AS curr WHERE new_row.customer_src_id = curr.customer_src_id AND
																	new_row.source_entity = curr.source_entity);
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
