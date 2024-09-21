-- CREATES PROCEDURE TO LOAD DATA FROM EXTERNAL SOURCES TO SRC OFFLINE SALES
CREATE OR REPLACE PROCEDURE BL_CL.SRC_OFFLINE_SALES_LOAD_CSV(IN file_abspath TEXT) 
LANGUAGE plpgsql
AS $$
DECLARE 
	proc_name TEXT = 'BL_CL.SRC_OFFLINE_LOAD_CSV';
	context TEXT; n_row INT = 0; err_detail TEXT;
	ext_offline_q TEXT := FORMAT(
		'CREATE FOREIGN TABLE IF NOT EXISTS SA_SALES_OFFLINE.EXT_SALES_OFFLINE
	(
		id VARCHAR(4000),
		order_id VARCHAR(4000),
		sales_date VARCHAR(4000),
		client_id VARCHAR(4000),
		fullname VARCHAR(4000),
		phone_number VARCHAR(4000),
		gender VARCHAR(4000),
		item_id VARCHAR(4000),
		item_name VARCHAR(4000),
		brand VARCHAR(4000),
		category_id VARCHAR(4000),
		category VARCHAR(4000),
		subcategory_id VARCHAR(4000),
		subcategory VARCHAR(4000),
		shop_name VARCHAR(4000),
		shop_address VARCHAR(4000),
		province_id VARCHAR(4000),
		province VARCHAR(4000),
		town_id VARCHAR(4000),
		town VARCHAR(4000),
		district_id VARCHAR(4000),
		district VARCHAR(4000),
		payment_method VARCHAR(4000),
		retail_price VARCHAR(4000),
		quantity VARCHAR(4000),
		sale_price VARCHAR(4000),
		emp_id VARCHAR(4000),
		emp_first_name VARCHAR(4000),
		emp_last_name VARCHAR(4000),
		emp_email VARCHAR(4000),
		emp_phone_number VARCHAR(4000),
		emp_gender VARCHAR(4000)
	) SERVER file_server
	OPTIONS (
			filename %L,
			format %L,
			header %L,
			delimiter %L
	);', $1, 'csv', 'true', ';');

BEGIN
	-- CREATE EXTERNAL TABLE FOR OFFLINE SOURCE
	DROP FOREIGN TABLE IF EXISTS SA_SALES_OFFLINE.EXT_SALES_OFFLINE;
	EXECUTE ext_offline_q;

	-- CREATE SOURCE TABLE FOR OFFLINE SOURCE
	CREATE TABLE IF NOT EXISTS SA_SALES_OFFLINE.SRC_SALES_OFFLINE (LIKE SA_SALES_OFFLINE.EXT_SALES_OFFLINE);
	
	-- ADD REFRESH COLUMN TO IMPLEMENT INCREMENTAL LOAD
	ALTER TABLE SA_SALES_OFFLINE.SRC_SALES_OFFLINE ADD COLUMN IF NOT EXISTS refresh_dt TIMESTAMP DEFAULT NOW();

	-- INSERT NEW ROWS FROM EXTERNAL TABLE
	INSERT INTO SA_SALES_OFFLINE.SRC_SALES_OFFLINE (
		id, order_id, sales_date,
		client_id, fullname, phone_number, gender,
		item_id, item_name, brand, category_id, category, subcategory_id, subcategory,
		shop_name, shop_address,
		province_id, province, town_id, town, district_id, district,
		payment_method,
		emp_id, emp_first_name, emp_last_name, emp_email, emp_phone_number, emp_gender,
		retail_price, quantity, sale_price)
	SELECT ext.id, ext.order_id, ext.sales_date, -- SALE DATA
			ext.client_id, ext.fullname, ext.phone_number, ext.gender, -- CUSTOMER DATA
			ext.item_id, ext.item_name, ext.brand, ext.category_id, ext.category, ext.subcategory_id, ext.subcategory, -- PRODUCTS DATA
			ext.shop_name, ext.shop_address, -- STORE DATA
			ext.province_id, ext.province, ext.town_id, ext.town, ext.district_id, ext.district, -- ADDRESS DATA
			ext.payment_method, -- PAYMENT METHOD DATA
			ext.emp_id, ext.emp_first_name, ext.emp_last_name, ext.emp_email, ext.emp_phone_number, ext.emp_gender, -- EMPLOYEE DATA
			ext.retail_price, ext.quantity, ext.sale_price -- MEASUREMENTS / FACTS DATA
	FROM SA_SALES_OFFLINE.EXT_SALES_OFFLINE AS ext
	EXCEPT
	SELECT id, order_id, sales_date,
			client_id, fullname, phone_number, gender,
			item_id, item_name, brand, category_id, category, subcategory_id, subcategory,
			shop_name, shop_address,
			province_id, province, town_id, town, district_id, district,
			payment_method,
			emp_id, emp_first_name, emp_last_name, emp_email, emp_phone_number, emp_gender,
			retail_price, quantity, sale_price
	FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE;

	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		n_row := ROW_COUNT;
	
	CALL BL_CL.log_procedure(proc_name, n_row);

	RAISE NOTICE 'Data loaded sucessfully. % Rows inserted', n_row;
															
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Data insertion failed';
			
			GET STACKED DIAGNOSTICS 
				context := PG_EXCEPTION_CONTEXT,
				err_detail := PG_EXCEPTION_DETAIL;
			
			CALL BL_CL.log_procedure(proc_name, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
			RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$;	