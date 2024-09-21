-- CREATE SOURCE TABLE FOR ONLINE SOURCE
DROP TABLE IF EXISTS SA_SALES_ONLINE.SRC_SALES_OFFLINE CASCADE;
CREATE TABLE IF NOT EXISTS SA_SALES_OFFLINE.SRC_SALES_OFFLINE
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
	emp_gender VARCHAR(4000),
	refresh_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DDL SCRIPT FOR INSERTING DATA FROM EXTERNAL SOURCE
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
FROM SA_SALES_OFFLINE.EXT_SALES_OFFLINE AS ext;

COMMIT;

--SELECT * FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE;