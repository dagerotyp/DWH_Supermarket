-- CREATE A COMPOSITE TYPE TO STORE DATA REGARDING DUPLICATE ROWS
DROP TYPE IF EXISTS BL_CL.PAYMENTS_3NF CASCADE;
CREATE TYPE BL_CL.PAYMENTS_3NF AS (
	payment_id VARCHAR,
	payment_method_name VARCHAR,
	source_system VARCHAR,
	source_entity VARCHAR
);

-- FUNCTION TO LOAD DATA TO DIM PAYMENTS TABLE WITH USE OF COMPOSITE TYPE. IT RETURNS ALL PAYMENTS WHICH ALREADY EXISTED AND WAS NOT INSERTED INTO TABLE
CREATE OR REPLACE FUNCTION BL_CL.DIM_PAYMENTS_FUNC_LOAD() 
RETURNS SETOF BL_CL.PAYMENTS_3NF -- COMPOSITE TYPE
LANGUAGE plpgsql
AS $$
DECLARE 
	func_name TEXT = 'BL_CL.DIM_PAYMENTS_FUNC_LOAD()';
	context TEXT; n_row INT = 0; err_detail TEXT;

	query TEXT := FORMAT( 
	'INSERT INTO BL_DM.DIM_PAYMENTS (payment_surr_id, payment_name, payment_src_id, source_system, source_entity)
	SELECT $1, $2, $3, %L, %L;', 'BL_3NF', 'CE_PAYMENT_METHODS');
	
	--LOOP-THROUGH CURSOR
	prod_3nf_cursor CURSOR FOR SELECT payment_method_id::VARCHAR AS payment_src_id, 
										payment_name, source_system, source_entity
								FROM BL_3NF.CE_PAYMENT_METHODS WHERE payment_method_id != -1;
	rec RECORD;
	res BL_CL.PAYMENTS_3NF; -- CUSTOM DATA TYPE!
BEGIN
	-- LOOP THROUGH CURSOR OUTPUT
	FOR rec IN prod_3nf_cursor LOOP
		IF NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PAYMENTS AS p WHERE p.payment_src_id = rec.payment_src_id) THEN

		-- EXECUTE DYNAMIC QUERY IF PAYMENT METHOD WAS NOT IN DIM_PAYMENTS
			EXECUTE query USING nextval('BL_DM.SEQ_DIM_PAYMENT_SURR_ID'), rec.payment_name, rec.payment_src_id;
			n_row := n_row + 1;
		ELSE
		-- IF PAYMENT METHOD ALREADY IN DIM_PAYMENTS ADD TO res (CUSTOM  DATA TYPE FOR STORING DUPLICATES OF QUERY)
			SELECT rec.payment_src_id, rec.payment_name, rec.source_system, rec.source_entity INTO res;
			RETURN NEXT res;
		END IF;
	END LOOP;
	
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