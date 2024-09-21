-- DROP TABLE IF EXISTS BL_CL.mta_missing_data;
CREATE TABLE IF NOT EXISTS BL_CL.mta_missing_data (
	table_name VARCHAR(255) NOT NULL UNIQUE,
	n_rows BIGINT NOT NULL DEFAULT 0,
	update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ADD ALL EXISTING TABLES IN BL_3NF AND BL_DM LAYER
INSERT INTO BL_CL.mta_missing_data (table_name)
SELECT t.tablename 
FROM pg_tables AS t
WHERE t.schemaname IN ('bl_3nf', 'bl_dm') AND 
	(t.tablename LIKE ('ce_%') OR t.tablename LIKE ('dim_%') OR t.tablename = 'fct_sales_dd')
	AND NOT EXISTS (SELECT 1 FROM BL_CL.mta_missing_data mmd WHERE mmd.table_name = t.tablename);
	
-- DROP TABLE IF EXISTS BL_CL.mta_unique_data;
CREATE TABLE IF NOT EXISTS BL_CL.mta_unique_data (
	table_name VARCHAR(255) NOT NULL UNIQUE,
	n_rows BIGINT NOT NULL DEFAULT 0,
	duplicate_values TEXT NOT NULL DEFAULT '',
	update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ADD ALL EXISTING TABLES IN BL_3NF AND BL_DM LAYER
INSERT INTO BL_CL.mta_unique_data (table_name)
SELECT t.tablename 
FROM pg_tables AS t
WHERE t.schemaname IN ('bl_3nf', 'bl_dm') AND 
	(t.tablename LIKE ('ce_%') OR t.tablename LIKE ('dim_%') OR t.tablename = 'fct_sales_dd')
	AND NOT EXISTS (SELECT 1 FROM BL_CL.mta_unique_data mmd WHERE mmd.table_name = t.tablename);

-- CREATE MAIN TABLE FOR TESTS
--DROP TABLE IF EXISTS BL_CL.mta_tests;
CREATE TABLE IF NOT EXISTS BL_CL.mta_tests (
	test_name VARCHAR(255) NOT NULL UNIQUE,
	test_desc VARCHAR(255) NOT NULL,
	test_query TEXT NOT NULL,
	test_result_table VARCHAR(255) NOT NULL,
	test_status VARCHAR(255) NOT NULL,
	update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO BL_CL.mta_tests (test_name, test_desc, test_query, test_result_table, test_status)
SELECT 'TEST FOR DUPLICATES',
		'CHECKS ALL TABLES FROM 3NF AND DM LAYER FOR DUPLICATE ROWS',
		'CALL BL_CL.mta_unique_data_test();',	
		'MTA_UNIQUE_DATA',
		'NOT PERFORMED'
UNION ALL
SELECT 'TEST FOR MISSING DATA',
		'CHECKS ALL TABLES FROM 3NF AND DM LAYER FOR MISSING VALUES FROM SOURCE TABLES',
		'CALL BL_CL.mta_missing_data_test();',	
		'MTA_MISSING_DATA',
		'NOT PERFORMED';

-- TEST PROCEDURE FOR CHECKING MISSING VALUES
CREATE OR REPLACE PROCEDURE BL_CL.mta_missing_data_test()
LANGUAGE plpgsql
AS $$
DECLARE
	province_m INT := 0; town_m INT := 0; district_m INT := 0; address_m INT := 0; product_category_m INT := 0; product_subcategory_m INT := 0; brand_m INT := 0; product_m INT := 0;
	customer_m INT := 0; delivery_method_m INT := 0; delivery_m INT := 0; payment_method_m INT := 0; employee_m INT := 0; store_m INT := 0; sale_m INT := 0;
	dim_customer_m INT := 0; dim_employee_m INT := 0; dim_payment_m INT := 0; dim_delivery_m INT := 0; dim_store_m INT := 0; dim_product_m INT := 0; fct_sale_m INT := 0;
BEGIN
-- UPDATE update_dt TO CURRENT TIME
	UPDATE BL_CL.mta_missing_data
	SET update_dt = CURRENT_TIMESTAMP;
	
	RAISE NOTICE 'CHECKING FOR MISSING DATA IN 3NF LAYER...';
-- TEST FOR MISSING DATA IN CE_PROVINCES (WITH DEDUPLICATION)
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT province FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION SELECT DISTINCT province FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_provinces WHERE province_id <> -1)
	INTO province_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = province_m
	WHERE table_name = 'ce_provinces';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_PROVINCES: %', province_m;

-- TEST FOR MISSING DATA IN CE_TOWNS (WITH DEDUPLICATION)
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT province, town FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION SELECT DISTINCT province, town FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_towns WHERE town_id <> -1)
	INTO town_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = town_m
	WHERE table_name = 'ce_towns';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_TOWNS: %', town_m;

-- TEST FOR MISSING DATA IN CE_TOWNS (WITH DEDUPLICATION)
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT province, town, district FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION SELECT DISTINCT province, town, district FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_districts WHERE district_id <> -1)
	INTO district_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = district_m
	WHERE table_name = 'ce_districts';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_DISTRICTS: %', district_m;

-- TEST FOR MISSING DATA IN CE_ADDRESSES
	SELECT (SELECT COUNT(DISTINCT delivery_address) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) +
			(SELECT COUNT(DISTINCT shop_address) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_3NF.ce_addresses WHERE address_id <> -1)
	INTO address_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = address_m
	WHERE table_name = 'ce_addresses';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_ADDRESSES: %', address_m;

	-- TEST FOR MISSING DATA IN CE_PRODUCT_CATEGORIES
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT category_id FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION ALL SELECT DISTINCT category_id FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_product_categories WHERE product_category_id <> -1)
	INTO product_category_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = product_category_m
	WHERE table_name = 'ce_product_categories';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_PRODUCT_CATEGORIES: %', product_category_m;

-- TEST FOR MISSING DATA IN CE_PRODUCT_SUBCATEGORIES
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT category_id, subcategory_id FROM SA_SALES_ONLINE.SRC_SALES_ONLINE)) + 
			(SELECT COUNT(*) FROM (SELECT DISTINCT category_id, subcategory_id FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_product_subcategories WHERE product_subcategory_id <> -1)
	INTO product_subcategory_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = product_subcategory_m
	WHERE table_name = 'ce_product_subcategories';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_PRODUCT_SUBCATEGORIES: %', product_subcategory_m;

-- TEST FOR MISSING DATA IN CE_BRAND
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT brand FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION ALL SELECT DISTINCT brand FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_brands WHERE brand_id <> -1)
	INTO brand_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = brand_m
	WHERE table_name = 'ce_brands';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_BRANDS: %', brand_m;

-- TEST FOR MISSING DATA IN CE_PRODUCTS
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT item_id FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION ALL SELECT DISTINCT item_id FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_products WHERE product_id <> -1)
	INTO product_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = product_m
	WHERE table_name = 'ce_products';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_PRODUCTS: %', product_m;

-- TEST FOR MISSING DATA IN CE_CUSTOMERS
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT client_id FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION ALL SELECT DISTINCT client_id FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_customers WHERE customer_id <> -1)
	INTO customer_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = customer_m
	WHERE table_name = 'ce_customers';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_CUSTOMERS: %', customer_m;

--TEST FOR MISSING DATA IN CE_DELIVERY_METHODS
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT delivery_method FROM SA_SALES_ONLINE.SRC_SALES_ONLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_delivery_methods WHERE delivery_method_id <> -1)
	INTO delivery_method_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = delivery_method_m
	WHERE table_name = 'ce_delivery_methods';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_DELIVERY_METHODS: %', delivery_method_m;

-- TEST FOR MISSING DATA IN CE_DELIVERIES
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT order_id FROM SA_SALES_ONLINE.SRC_SALES_ONLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_deliveries WHERE delivery_id <> -1)
	INTO delivery_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = delivery_m
	WHERE table_name = 'ce_deliveries';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_DELIVERIES: %', delivery_m;

-- TEST FOR MISSING DATA IN CE_PAYMENT_METHODS
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT payment_method FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION ALL SELECT DISTINCT payment_method FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_payment_methods WHERE payment_method_id <> -1)
	INTO payment_method_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = payment_method_m
	WHERE table_name = 'ce_payment_methods';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_PAYMENT_METHODS: %', payment_method_m;

-- TEST FOR MISSING DATA IN CE_EMPLOYEES_SCD
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT emp_id FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_employees_scd WHERE employee_id <> -1 AND is_active = TRUE)
	INTO employee_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = employee_m
	WHERE table_name = 'ce_employees_scd';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_EMPLOYEES_SCD: %', employee_m;

-- TEST FOR MISSING DATA IN CE_STORES
	SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT shop_email FROM SA_SALES_ONLINE.SRC_SALES_ONLINE UNION ALL SELECT DISTINCT shop_name FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE)) -
			(SELECT COUNT(*) FROM BL_3NF.ce_stores WHERE store_id <> -1)
	INTO store_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = store_m
	WHERE table_name = 'ce_stores';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_STORES: %', store_m;

--TEST FOR MISSING DATA IN CE_SALES
	SELECT (SELECT COUNT(*) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) +
			(SELECT COUNT(*) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_3NF.ce_sales)
	INTO sale_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = sale_m
	WHERE table_name = 'ce_sales';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN CE_SALES: %', sale_m;

	RAISE NOTICE 'CHECKING FOR MISSING DATA IN DM LAYER...';
-- TEST FOR MISSING DATA IN DIM_CUSTOMERS
	SELECT (SELECT COUNT(DISTINCT client_id) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) + 
			(SELECT COUNT(DISTINCT client_id) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_DM.dim_customers WHERE customer_surr_id <> -1)
	INTO dim_customer_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = dim_customer_m
	WHERE table_name = 'dim_customers';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN DIM_CUSTOMERS: %', dim_customer_m;

-- TEST FOR MISSING DATA IN DIM_EMPLOYEES_SCD
	SELECT (SELECT COUNT(DISTINCT emp_id) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_DM.dim_employees_scd WHERE employee_surr_id <> -1 AND is_active = TRUE)
	INTO dim_employee_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = dim_employee_m
	WHERE table_name = 'dim_employees_scd';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN DIM_EMPLOYEES_SCD: %', dim_employee_m;

-- TEST FOR MISSING DATA IN DIM_PAYMENTS
	SELECT (SELECT COUNT(DISTINCT payment_method) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) + 
			(SELECT COUNT(DISTINCT payment_method) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_DM.dim_payments WHERE payment_surr_id <> -1)
	INTO dim_payment_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = dim_payment_m
	WHERE table_name = 'dim_payments';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN DIM_PAYMENTS: %', dim_payment_m;

-- TEST FOR MISSING DATA IN DIM_DELIVERIES
	SELECT (SELECT COUNT(DISTINCT order_id) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) -
			(SELECT COUNT(*) FROM BL_DM.dim_deliveries WHERE delivery_surr_id <> -1)
	INTO dim_delivery_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = dim_delivery_m
	WHERE table_name = 'dim_deliveries';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN DIM_DELIVERIES: %', dim_delivery_m;

-- TEST FOR MISSING DATA IN DIM_PRODUCTS
	SELECT (SELECT COUNT(DISTINCT item_id) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) + 
			(SELECT COUNT(DISTINCT item_id) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_DM.dim_products WHERE product_surr_id <> -1)
	INTO dim_product_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = dim_product_m
	WHERE table_name = 'dim_products';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN DIM_PRODUCTS: %', dim_product_m;

-- TEST FOR MISSING DATA IN DIM_STORES
	SELECT (SELECT COUNT(DISTINCT shop_email) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) + 
			(SELECT COUNT(DISTINCT shop_name) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_DM.dim_stores WHERE store_surr_id <> -1)
	INTO dim_store_m;
	
	UPDATE BL_CL.mta_missing_data
	SET n_rows = dim_store_m
	WHERE table_name = 'dim_stores';
	
	RAISE NOTICE 'NUMBER OF MISSING ROWS IN DIM_STORES: %', dim_store_m;

-- TEST FOR MISSING DATA IN FCT_SALES_DD
	SELECT (SELECT COUNT(*) FROM SA_SALES_ONLINE.SRC_SALES_ONLINE) + 
			(SELECT COUNT(*) FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE) -
			(SELECT COUNT(*) FROM BL_DM.fct_sales_dd)
	INTO fct_sale_m;

	UPDATE BL_CL.mta_missing_data
	SET n_rows = fct_sale_m
	WHERE table_name = 'fct_sales_dd';

	RAISE NOTICE 'NUMBER OF MISSING ROWS IN FCT_SALES_DD: %', fct_sale_m;
	RAISE NOTICE 'TEST FOR MISSING DATA COMPLETED';
END;
$$;

-- TEST PROCEDURE FOR CHECKING DUPLICATES
CREATE OR REPLACE PROCEDURE BL_CL.mta_unique_data_test()
LANGUAGE plpgsql
AS $$	
BEGIN
	UPDATE BL_CL.mta_unique_data
	SET update_dt = CURRENT_TIMESTAMP;
	
	RAISE NOTICE 'CHECKING FOR DUPLICATES IN 3NF LAYER...';

-- CHECK FOR DUPLICATES IN CE_PROVINCES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_PROVINCES';
	WITH duplicates AS (
		SELECT province_src_id AS duplicated_rows
		FROM BL_3NF.CE_PROVINCES
		GROUP BY province_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
	SET n_rows = (SELECT COUNT(*) FROM duplicates),
		duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_provinces';

-- CHECK FOR DUPLICATES IN CE_TOWNS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_TOWNS';
	WITH duplicates AS (
		SELECT town_src_id AS duplicated_rows
		FROM BL_3NF.CE_TOWNS
		GROUP BY town_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
	SET n_rows = (SELECT COUNT(*) FROM duplicates),
		duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_towns';

-- CHECK FOR DUPLICATES IN CE_DISTRICTS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_DISTRICTS';
	WITH duplicates AS (
		SELECT district_src_id AS duplicated_rows
		FROM BL_3NF.CE_DISTRICTS
		GROUP BY district_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
	SET n_rows = (SELECT COUNT(*) FROM duplicates),
		duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_districts';

-- CHECK FOR DUPLICATES IN CE_ADDRESSES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_ADDRESSES';
	WITH duplicates AS (
		SELECT address_src_id AS duplicated_rows
		FROM BL_3NF.CE_ADDRESSES
		GROUP BY address_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
		duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_addresses';

-- CHECK FOR DUPLICATES IN CE_PRODUCT_CATEGORIES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN PRODUCT_CATEGORIES';
	WITH duplicates AS (
		SELECT product_category_src_id AS duplicated_rows
		FROM BL_3NF.CE_PRODUCT_CATEGORIES
		GROUP BY product_category_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
	SET n_rows = (SELECT COUNT(*) FROM duplicates),
		duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_product_categories';

-- CHECK FOR DUPLICATES IN CE_PRODUCT_SUBCATEGORIES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN PRODUCT_SUBCATEGORIES';
	WITH duplicates AS (
		SELECT product_subcategory_src_id AS duplicated_rows
		FROM BL_3NF.CE_PRODUCT_SUBCATEGORIES
		GROUP BY product_subcategory_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
	SET n_rows = (SELECT COUNT(*) FROM duplicates),
		duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_product_subcategories';

-- CHECK FOR DUPLICATES IN CE_BRANDS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_BRANDS';
	WITH duplicates AS (
		SELECT brand_src_id AS duplicated_rows
		FROM BL_3NF.CE_BRANDS
		GROUP BY brand_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_brands';

-- CHECK FOR DUPLICATES IN CE_PRODUCTS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_PRODUCTS';
	WITH duplicates AS (
		SELECT product_src_id AS duplicated_rows
		FROM BL_3NF.CE_PRODUCTS
		GROUP BY product_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_products';

-- CHECK FOR DUPLICATES IN CE_PRODUCTS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_CUSTOMERS';
	WITH duplicates AS (
		SELECT customer_src_id AS duplicated_rows
		FROM BL_3NF.CE_CUSTOMERS
		GROUP BY customer_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_customers';

-- CHECK FOR DUPLICATES IN CE_PAYMENT_METHODS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_PAYMENT_METHODS';
	WITH duplicates AS (
		SELECT payment_method_src_id AS duplicated_rows
		FROM BL_3NF.CE_PAYMENT_METHODS
		GROUP BY payment_method_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_payment_methods';

-- CHECK FOR DUPLICATES IN CE_DELIVERY_METHODS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_DELIVERY_METHODS';
	WITH duplicates AS (
		SELECT delivery_method_src_id AS duplicated_rows
		FROM BL_3NF.CE_DELIVERY_METHODS
		GROUP BY delivery_method_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_delivery_methods';

-- CHECK FOR DUPLICATES IN CE_DELIVERIES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_DELIVERIES';
	WITH duplicates AS (
		SELECT delivery_src_id AS duplicated_rows
		FROM BL_3NF.CE_DELIVERIES
		GROUP BY delivery_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_deliveries';

-- CHECK FOR DUPLICATES IN CE_EMYPLOEES_SCD
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_EMPLOYEES_SCD';
	WITH duplicates AS (
		SELECT employee_src_id AS duplicated_rows
		FROM BL_3NF.CE_EMPLOYEES_SCD
		WHERE is_active = TRUE
		GROUP BY employee_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_employees_scd';

-- CHECK FOR DUPLICATES IN CE_STORES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_STORES';
	WITH duplicates AS (
		SELECT store_src_id AS duplicated_rows
		FROM BL_3NF.CE_STORES
		GROUP BY store_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_stores';

-- CHECK FOR DUPLICATES IN CE_STORES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN CE_SALES';
	WITH duplicates AS (
		SELECT sale_src_id AS duplicated_rows
		FROM BL_3NF.CE_SALES
		GROUP BY sale_src_id, event_dt, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'ce_sales';

	RAISE NOTICE 'CHECKING FOR DUPLICATES IN DM LAYER...';

-- CHECK FOR DUPLICATES IN DIM_CUSTOMERS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN DIM_CUSTOMERS';
	WITH duplicates AS (
		SELECT customer_src_id AS duplicated_rows
		FROM BL_DM.DIM_CUSTOMERS
		GROUP BY customer_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'dim_customers';

-- CHECK FOR DUPLICATES IN DIM_EMPLOYEES_SCD
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN DIM_EMPLOYEES_SCD';
	WITH duplicates AS (
		SELECT employee_src_id AS duplicated_rows
		FROM BL_DM.DIM_EMPLOYEES_SCD
		WHERE is_active = TRUE
		GROUP BY employee_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'dim_employees_scd';

-- CHECK FOR DUPLICATES IN DIM_PAYMENTS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN DIM_PAYMENTS';
	WITH duplicates AS (
		SELECT payment_src_id AS duplicated_rows
		FROM BL_DM.DIM_PAYMENTS
		GROUP BY payment_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'dim_payments';

-- CHECK FOR DUPLICATES IN DIM_DELIVERIES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN DIM_DELIVERIES';
	WITH duplicates AS (
		SELECT delivery_src_id AS duplicated_rows
		FROM BL_DM.DIM_DELIVERIES
		GROUP BY delivery_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'dim_deliveries';

-- CHECK FOR DUPLICATES IN DIM_PRODUCTS
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN DIM_PRODUCTS';
	WITH duplicates AS (
		SELECT product_src_id AS duplicated_rows
		FROM BL_DM.DIM_PRODUCTS
		GROUP BY product_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'dim_products';

-- CHECK FOR DUPLICATES IN DIM_STORES
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN DIM_STORES';
	WITH duplicates AS (
		SELECT store_src_id AS duplicated_rows
		FROM BL_DM.DIM_STORES
		GROUP BY store_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'dim_stores';

-- CHECK FOR DUPLICATES IN FCT_SALES_DD
	RAISE NOTICE 'CHECKING FOR DUPLICATES ROWS IN FCT_SALES_DD';
	WITH duplicates AS (
		SELECT sale_src_id AS duplicated_rows
		FROM BL_DM.FCT_SALES_DD
		GROUP BY sale_src_id, source_entity
		HAVING COUNT(*) > 1
	)
	UPDATE BL_CL.mta_unique_data
		SET n_rows = (SELECT COUNT(*) FROM duplicates),
			duplicate_values = (SELECT COALESCE(array_agg(duplicated_rows)::TEXT, 'NO DUPLICATES') FROM duplicates)
	WHERE table_name = 'fct_sales_dd';

END;
$$;

-- MASTER TEST PROCEDURE FOR ALL TESTS
CREATE OR REPLACE PROCEDURE BL_CL.mta_master_test()
LANGUAGE plpgsql
AS $$
DECLARE 
	test_cursor CURSOR FOR SELECT * FROM BL_CL.mta_tests;
	test record;
	cnt INT;
BEGIN 
	RAISE NOTICE 'STARTING TEST PROCEDURES TO CHECK FOR DUPLICATES AND MISSING ROWS, %', CURRENT_TIMESTAMP;

	FOR test IN test_cursor LOOP
		RAISE NOTICE 'STARTING %. %', test.test_name, test.test_desc;
	
		EXECUTE test.test_query;
	
		EXECUTE 'SELECT SUM(n_rows) FROM BL_CL.' || test.test_result_table INTO cnt;
	
		IF cnt <> 0 THEN
			UPDATE BL_CL.mta_tests
			SET test_status = 'TEST FAILED',
				update_dt = CURRENT_TIMESTAMP
			WHERE test_name = test.test_name;
			
		ELSE
			UPDATE BL_CL.mta_tests
			SET test_status = 'TEST PASSED',
				update_dt = CURRENT_TIMESTAMP
			WHERE test_name = test.test_name;
		END IF;
	
		RAISE NOTICE '% IS COMPLETED. SEE % FOR DETAILS', test.test_name, test.test_result_table;
		
	END LOOP;

	RAISE NOTICE 'TEST PROCEDURES FINISHED, %', CURRENT_TIMESTAMP;

END;
$$;


