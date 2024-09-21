-- ADDING EXTENSION FOR FOREGIN TABLES AND CREATEA A SERVER
CREATE EXTENSION IF NOT EXISTS file_fdw SCHEMA BL_CL;
CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;

CREATE SCHEMA IF NOT EXISTS SA_SALES_OFFLINE;

-- CREATE EXTERNAL TABLE FOR ONLINE SOURCE
DROP FOREIGN TABLE IF EXISTS SA_SALES_OFFLINE.EXT_SALES_OFFLINE;
CREATE FOREIGN TABLE IF NOT EXISTS SA_SALES_OFFLINE.EXT_SALES_OFFLINE
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
OPTIONS (format 'csv', filename 'C:\Users\Paweł\OneDrive\EPAM FILES\Final-Project\DATA SOURCES\SALES_OFFLINE.CSV', header 'true', delimiter ';');
