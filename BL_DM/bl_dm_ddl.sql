-- GENERATE SEQUENCE FOR PK OF DIM_CUSTOMERS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_CUSTOMER_SURR_ID START 1;

-- CREATE DIM_CUSTOMERS TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_CUSTOMERS (
	customer_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_CUSTOMER_SURR_ID'),
	first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	phone_number VARCHAR(255) NOT NULL,
	gender VARCHAR(255) NOT NULL,
	birthdate_dt DATE NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	customer_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_CUSTOMERS
ALTER SEQUENCE BL_DM.SEQ_DIM_CUSTOMER_SURR_ID OWNED BY BL_DM.DIM_CUSTOMERS.CUSTOMER_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_CUSTOMERS (customer_surr_id, first_name, last_name, email, phone_number, gender, birthdate_dt, customer_src_id, source_system, source_entity)
SELECT -1 AS customer_surr_id,
		'n. a.' AS first_name,
		'n. a.' AS last_name,
		'n. a.' AS email,
		'n. a.' AS phone_number,
		'n. a.' AS gender,
		'1990-01-01'::DATE AS birthdate_dt,
		'n. a.' AS customer_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_CUSTOMERS AS curr WHERE curr.customer_surr_id = -1);													
COMMIT;

-- GENERATE SEQUENCE FOR PK OF DIM_DELIVERIES
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_DELIVERY_SURR_ID START 1;

-- CREATE DIM_DELIVERIES TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_DELIVERIES (
	delivery_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_DELIVERY_SURR_ID'),
	delivery_method_id BIGINT NOT NULL,
	delivery_method_name VARCHAR(255) NOT NULL,
	address_id BIGINT NOT NULL,
	address VARCHAR(255) NOT NULL,
	district_id BIGINT NOT NULL,
	district_name VARCHAR(255) NOT NULL,
	town_id BIGINT NOT NULL,
	town_name VARCHAR(255) NOT NULL,
	province_id BIGINT NOT NULL,
	province_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	delivery_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_DELIVERIES
ALTER SEQUENCE BL_DM.SEQ_DIM_DELIVERY_SURR_ID OWNED BY BL_DM.DIM_DELIVERIES.DELIVERY_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_DELIVERIES (delivery_surr_id, delivery_method_id, delivery_method_name, address_id, address, district_id, district_name,
								town_id, town_name, province_id, province_name, delivery_src_id, source_system, source_entity)
SELECT -1 AS delivery_surr_id,
		-1 AS delivery_method_id,
		'n. a.' AS delivery_method_name,
		-1 AS address_id,
		'n. a.' AS address,
		-1 AS district_id,
		'n. a.' AS district_name,
		-1 AS town_id,
		'n. a.' AS town_name,
		-1 AS province_id,
		'n. a.' AS province_name,
		'n. a.' AS delivery_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_DELIVERIES AS curr WHERE curr.delivery_surr_id = -1);														
COMMIT;

-- GENERATE SEQUENCE FOR PK OF DIM_EMPLOYEES_SCD
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID START 1;

-- CREATE DIM_EMPLOYEES_SCD TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_EMPLOYEES_SCD (
	employee_surr_id BIGINT DEFAULT nextval('BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID'),
	first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	phone_number VARCHAR(255) NOT NULL,
	gender VARCHAR(255)NOT NULL,
	start_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	end_dt TIMESTAMP NOT NULL DEFAULT '9999-12-31'::TIMESTAMP,
	is_active BOOL NOT NULL DEFAULT TRUE,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	employee_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (employee_surr_id, start_dt)
);

-- CONNECT SEQUENCE WITH PK OF DIM_DELIVERIES
ALTER SEQUENCE BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID OWNED BY BL_DM.DIM_EMPLOYEES_SCD.EMPLOYEE_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_EMPLOYEES_SCD (employee_surr_id, first_name, last_name, email, phone_number, gender, employee_src_id, source_system, source_entity)
SELECT -1 AS employee_surr_id,
		'n. a.' AS first_name,
		'n. a.' AS last_name,
		'n. a.' AS email,
		'n. a.' AS phone_number,
		'n. a.' AS gender,
		'-1' AS employee_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_EMPLOYEES_SCD WHERE employee_surr_id = -1);
COMMIT;

-- GENERATE SEQUENCE FOR PK OF DIM_PAYMENTS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_PAYMENT_SURR_ID START 1;

-- CREATE DIM_PAYMENTS TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PAYMENTS (
	payment_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_PAYMENT_SURR_ID'),
	payment_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	payment_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_PAYMENTS
ALTER SEQUENCE BL_DM.SEQ_DIM_PAYMENT_SURR_ID OWNED BY BL_DM.DIM_PAYMENTS.PAYMENT_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_PAYMENTS (payment_surr_id, payment_name, payment_src_id, source_system, source_entity)
SELECT -1 AS payment_surr_id,
		'n. a.' AS payment_name,
		'n. a.' AS payment_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PAYMENTS AS curr WHERE curr.payment_surr_id = -1);														
COMMIT;

-- GENERATE SEQUENCE FOR PK OF DIM_PRODUCTS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_PRODUCT_SURR_ID START 1;

-- CREATE DIM_PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PRODUCTS (
	product_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_PRODUCT_SURR_ID'),
	brand_id BIGINT NOT NULL,
	brand_name VARCHAR(255) NOT NULL,
	product_subcategory_id BIGINT NOT NULL,
	product_subcategory_name VARCHAR(255) NOT NULL,
	product_category_id BIGINT NOT NULL,
	product_category_name VARCHAR(255) NOT NULL,
	product_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	product_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_PRODUCTS
ALTER SEQUENCE BL_DM.SEQ_DIM_PRODUCT_SURR_ID OWNED BY BL_DM.DIM_PRODUCTS.PRODUCT_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_PRODUCTS (product_surr_id, brand_id, brand_name, product_subcategory_id, product_subcategory_name, product_category_id,
	product_category_name, product_name, product_src_id, source_system, source_entity)
SELECT -1 AS product_surr_id,
		-1 AS brand_id,
		'n. a.' AS brand_name,
		-1 AS product_subcategory_id,
		'n. a.' AS product_subcategory_name,
		-1 AS product_category_id,
		'n. a.' AS product_category_name,
		'n. a.' AS product_name,
		'n. a.' AS product_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PRODUCTS WHERE product_surr_id = -1);											
COMMIT;

-- GENERATE SEQUENCE FOR PK OF DIM_STORES
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_STORE_SURR_ID START 1;

-- CREATE DIM_STORES TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_STORES (
	store_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_STORE_SURR_ID'),
	address_id BIGINT NOT NULL,
	address VARCHAR(255) NOT NULL,
	district_id BIGINT NOT NULL,
	district_name VARCHAR(255) NOT NULL ,
	town_id BIGINT NOT NULL,
	town_name VARCHAR(255) NOT NULL,
	province_id BIGINT NOT NULL,
	province_name VARCHAR(255) NOT NULL,
	store_name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	website VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	store_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_STORES
ALTER SEQUENCE BL_DM.SEQ_DIM_STORE_SURR_ID OWNED BY BL_DM.DIM_STORES.STORE_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_STORES (store_surr_id, address_id, address, district_id, district_name, town_id, town_name, province_id, province_name, store_name,
								email, website, store_src_id, source_system, source_entity)
SELECT -1 AS store_surr_id,
		-1 AS address_id,
		'n. a.' AS address,
		-1 AS district_id,
		'n. a.' AS district_name,
		-1 AS town_id,
		'n. a.' AS town_name,
		-1 AS province_id,
		'n. a.' AS province_name,
		'n. a.' AS store_name,
		'n. a.' AS email,
		'n. a.' AS website,
		'n. a.' AS store_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_STORES AS curr WHERE curr.store_surr_id = -1);
COMMIT;

-- CREATE DIM_TIMES_DAY TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_TIME_DAY ( -- Currently WITHOUT SCHEMA name
	date_id DATE PRIMARY KEY,
	day_of_year_number INT,
	day_of_month_number INT,
	day_of_week_number INT,
	day_name VARCHAR(9),
	weekend_flag BOOL,
	week_of_year_number INT,
	month_value INT,
	month_name VARCHAR(9),
	year_month VARCHAR(7),
	quarter_value INT,
	year_quarter VARCHAR(7),
	year_value INT
);

INSERT INTO BL_DM.DIM_TIME_DAY (date_id, 
						day_of_year_number,
						day_of_month_number, 
						day_of_week_number,
						day_name, 
						weekend_flag,
						week_of_year_number, 
						month_value, 
						month_name,
						year_month,
						quarter_value, 
						year_quarter,
						year_value)		
SELECT DATE(dd),
		EXTRACT('doy' FROM dd)::INT, -- Number OF DAY IN YEAR (1-365)
		TO_CHAR(dd, 'DD')::INT, -- Number OF DAY IN MONTH (1-31)
		TO_CHAR(dd, 'ID')::INT, -- Number OF DAY IN week (1-7)
		TO_CHAR(dd, 'DAY'),		-- Name OF a DAY (Monday, Thursday, ...)
		CASE WHEN TO_CHAR(dd, 'ID') IN ('6', '7') THEN TRUE
			ELSE FALSE END,			-- Flag whether DAY IS a weekend (1) OR NOT (0)
		TO_CHAR(dd, 'IW')::INT, -- Number OF week IN year
		TO_CHAR(dd, 'MM')::INT, -- Number OF MONTH IN year
		TO_CHAR(dd, 'MONTH'),   -- Name OF a month
		TO_CHAR(dd, 'YYYY-MM'), -- Year and month formated AS YYYY-MM
		TO_CHAR(dd, 'Q')::INT,  -- Number OF quarter IN year
		TO_CHAR(dd, 'YYYY-Q'), 	-- Year and quarter formated AS YYYY-Q
		TO_CHAR(dd, 'YYYY')::INT -- Number OF year
FROM generate_series
        ( '2020-01-01'::TIMESTAMP
        , '2040-12-31'::TIMESTAMP
        , '1 day'::interval) dd
ON CONFLICT DO NOTHING;
COMMIT;


/*
CREATE OF FCT_SALES_DD TABLE

1) FACT TABLE DOES NOT CONTAIN DEFUALT ROWS
2) FK IN FACT TABLE ARE OPTIONAL. DROPPED IN THIS CASE
3) PARTITIONING STRATEGY IS A RANGE BY DATE WITH EACH PARTITION BEING MONTH IN YEAR. PARTITION WILL BE CREATED DYNAMICLY DURING LOADING OF DATA

*/
-- GENERATE SEQUENCE FOR PK OF DIM_DELIVERIES
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_FCT_SALES_DD_SURR_ID;

-- CREATE DIM_DELIVERIES TABLE
CREATE TABLE IF NOT EXISTS BL_DM.FCT_SALES_DD (
	sale_surr_id BIGINT NOT NULL DEFAULT nextval('BL_DM.SEQ_FCT_SALES_DD_SURR_ID'),
	order_id BIGINT NOT NULL,
	event_dt DATE NOT NULL,
	customer_id BIGINT NOT NULL, --REFERENCES BL_DM.DIM_CUSTOMERS (customer_surr_id) ,
	payment_method_id BIGINT NOT NULL, --REFERENCES BL_DM.DIM_PAYMENTS (payment_surr_id),
	delivery_id BIGINT NOT NULL, --REFERENCES BL_DM.DIM_DELIVERIES (delivery_surr_id),
	store_id BIGINT NOT NULL, --REFERENCES BL_DM.DIM_STORES (store_surr_id),
	product_id BIGINT NOT NULL, --REFERENCES BL_DM.DIM_PRODUCTS (product_surr_id),
	employee_id BIGINT NOT NULL,
	delivery_fee NUMERIC(10,2),
	retail_price NUMERIC(10,2),
	quantity INT,
	sale_price NUMERIC(10, 2),
	fct_total_price NUMERIC(10, 2),
	fct_discount FLOAT,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	sale_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_dt);

-- CONNECT SEQUENCE WITH PK OF DIM_DELIVERIES
ALTER SEQUENCE BL_DM.SEQ_FCT_SALES_DD_SURR_ID OWNED BY BL_DM.FCT_SALES_DD.SALE_SURR_ID;