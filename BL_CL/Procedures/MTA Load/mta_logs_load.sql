-- CREATES PROCEDURE TO ADD LOGS TO MTA_LOGS TABLE
CREATE OR REPLACE PROCEDURE BL_CL.log_procedure (IN p_proc_name TEXT, IN p_rows INT, IN p_log_message TEXT DEFAULT 'SUCCESFUL LOAD')
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO BL_CL.mta_logs (procedure_name, n_records, log_message)
	VALUES (p_proc_name, p_rows, p_log_message);
END;
$$;