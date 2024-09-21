-- ADDING EXTENSION FOR FOREGIN TABLES AND CREATEA A SERVER
CREATE EXTENSION IF NOT EXISTS file_fdw SCHEMA BL_CL;
CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;

CREATE SCHEMA IF NOT EXISTS SA_SALES_ONLINE;

-- CREATE EXTERNAL TABLE FOR ONLINE SOURCE
DROP FOREIGN TABLE IF EXISTS SA_SALES_ONLINE.EXT_SALES_ONLINE;
CREATE FOREIGN TABLE IF NOT EXISTS SA_SALES_ONLINE.EXT_SALES_ONLINE
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
OPTIONS (format 'csv', filename 'D:\DWH\Task1\SALES_ONLINE.CSV', header 'true', delimiter ';');

