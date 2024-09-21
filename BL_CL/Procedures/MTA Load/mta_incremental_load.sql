-- CREATES A PROCEDURE TO STORE INCREMENTAL LOAD DATA
CREATE OR REPLACE PROCEDURE BL_CL.INCREMENTAL_LOAD_UPDATE(
	src_table_name VARCHAR,
	tgt_table_name VARCHAR,
	proc_name VARCHAR,
	load_ts TIMESTAMP
)
LANGUAGE plpgsql
AS $$ 
BEGIN
	IF EXISTS (
		SELECT 1 FROM BL_CL.MTA_INCREMENTAL_LOAD  WHERE source_table_name = src_table_name AND
														target_table_name = tgt_table_name AND
														procedure_name = proc_name) THEN
		UPDATE BL_CL.MTA_INCREMENTAL_LOAD
		SET	latest_load_ts = load_ts
		WHERE src_table_name = src_table_name AND
				target_table_name = tgt_table_name AND
				procedure_name = proc_name;
	ELSE
		INSERT INTO BL_CL.MTA_INCREMENTAL_LOAD
		SELECT src_table_name, tgt_table_name, proc_name, load_ts;
	END IF;
END;
$$;

-- INITIAL LOAD TO INCREMENTAL LOAD TABLE WHICH LATER WILL BE UPDATED
CALL BL_CL.INCREMENTAL_LOAD_UPDATE('SRC_SALES_ONLINE'::TEXT, 'CE_SALES'::TEXT, 'CE_SALES_LOAD'::TEXT, '1900-01-01'::TIMESTAMP);
CALL BL_CL.INCREMENTAL_LOAD_UPDATE('SRC_SALES_OFFLINE'::TEXT, 'CE_SALES'::TEXT, 'CE_SALES_LOAD'::TEXT, '1900-01-01'::TIMESTAMP);