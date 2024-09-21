-- CREATES PROCEDURE TO LOAD DATA FROM EXTERNAL SOURCES TO SRC ONLINE SALES
CREATE OR REPLACE PROCEDURE BL_CL.SRC_ONLINE_SALES_LOAD_CSV(IN file_abspath TEXT) 
LANGUAGE plpgsql
AS $$
DECLARE 
	proc_name TEXT = 'BL_CL.SRC_ONLINE_LOAD_CSV';
	context TEXT; n_row INT = 0; err_detail TEXT;
	ext_offline_q TEXT := FORMAT(
		'CREATE FOREIGN TABLE IF NOT EXISTS SA_SALES_ONLINE.EXT_SALES_ONLINE
	(
		id VARCHAR(4000),
		order_id VARCHAR(4000),
		sales_date VARCHAR(4000),
		client_id VARCHAR(4000),
		first_name VARCHAR(4000),
		last_name VARCHAR(4000),
		email VARCHAR(4000),
		gender VARCHAR(4000),
		client_birthdate VARCHAR(4000),
		item_id VARCHAR(4000),
		item_name VARCHAR(4000),
		brand VARCHAR(4000),
		category_id VARCHAR(4000),
		category VARCHAR(4000),
		subcategory_id VARCHAR(4000),
		subcategory VARCHAR(4000),
		shop_online_address VARCHAR(4000),
		shop_email VARCHAR(4000),
		payment_method VARCHAR(4000),
		delivery_address VARCHAR(4000),
		province_id VARCHAR(4000),
		province VARCHAR(4000),
		town_id VARCHAR(4000),
		town VARCHAR(4000),
		district_id VARCHAR(4000),
		district VARCHAR(4000),
		delivery_method VARCHAR(4000),
		delivery_fee VARCHAR(4000),
		retail_price VARCHAR(4000),
		quantity VARCHAR(4000),
		sale_price VARCHAR(4000)
	) SERVER file_server
	OPTIONS (
			filename %L,
			format %L,
			header %L,
			delimiter %L
	);', $1, 'csv', 'true', ';');

BEGIN
	-- CREATE EXTERNAL TABLE FOR ONLINE SOURCE
	DROP FOREIGN TABLE IF EXISTS SA_SALES_ONLINE.EXT_SALES_ONLINE;
	EXECUTE ext_offline_q;

	-- CREATE SOURCE TABLE FOR ONLINE SOURCE
	CREATE TABLE IF NOT EXISTS SA_SALES_ONLINE.SRC_SALES_ONLINE (LIKE SA_SALES_ONLINE.EXT_SALES_ONLINE);
	
	-- ADD REFRESH COLUMN TO IMPLEMENT INCREMENTAL LOAD
	ALTER TABLE SA_SALES_ONLINE.SRC_SALES_ONLINE ADD COLUMN IF NOT EXISTS refresh_dt TIMESTAMP DEFAULT NOW();

	-- INSERT NEW ROWS FROM EXTERNAL TABLE
	INSERT INTO SA_SALES_ONLINE.SRC_SALES_ONLINE (
		id, order_id, sales_date, client_id,
		first_name, last_name, email, gender, client_birthdate,
		item_id, item_name, brand, category_id, category, subcategory_id, subcategory,
		shop_online_address, shop_email,
		payment_method, 
		delivery_address, delivery_method,
		province_id, province, town_id, town, district_id, district,
		delivery_fee, retail_price, quantity, sale_price)
	SELECT ext.id, ext.order_id, ext.sales_date,
			ext.client_id, ext.first_name, ext.last_name, ext.email, ext.gender, ext.client_birthdate, -- CLIENT DATA
			ext.item_id, ext.item_name, ext.brand, ext.category_id, ext.category, ext.subcategory_id, ext.subcategory, -- PRODUCTS DATA
			ext.shop_online_address,ext.shop_email, -- STORE DATA
			ext.payment_method, -- PAYMENT METHOD DATA
			ext.delivery_address, ext.delivery_method, -- DELIVERY DATA
			ext.province_id, ext.province, ext.town_id, ext.town, ext.district_id, ext.district, -- ADDRESS DATA
			ext.delivery_fee,ext.retail_price,ext.quantity,ext.sale_price -- MEASUREMENTS / FACTS,
	FROM SA_SALES_ONLINE.EXT_SALES_ONLINE AS ext
	EXCEPT 
	SELECT id, order_id, sales_date, client_id,
		first_name, last_name, email, gender, client_birthdate,
		item_id, item_name, brand, category_id, category, subcategory_id, subcategory,
		shop_online_address, shop_email,
		payment_method, 
		delivery_address, delivery_method,
		province_id, province, town_id, town, district_id, district,
		delivery_fee, retail_price, quantity, sale_price
	FROM SA_SALES_ONLINE.SRC_SALES_ONLINE;

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