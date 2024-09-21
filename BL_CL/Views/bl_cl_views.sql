-- VIEW THAT CHECKS IF THERE IS NEW DATA SINCE LAST LOAD
CREATE OR REPLACE VIEW BL_CL.SALES_INCREMENT AS 
SELECT id, order_id, sales_date, client_id, first_name, last_name, 'n. a.' AS phone_number, email, gender, client_birthdate,
		item_id, item_name, brand, category_id, category, subcategory_id, subcategory,
		'n. a.' AS shop_name, 'n. a.' AS shop_address, shop_online_address, shop_email,
		province_id, province, town_id, town, district_id, district,
		payment_method, 
		delivery_address, delivery_method,
		delivery_fee, retail_price, quantity, sale_price,
		'n. a.' AS emp_id, 'n. a.' AS emp_first_name, 'n. a.' AS emp_last_name, 'n. a.' AS emp_email, 'n. a.' AS emp_phone_number, 'n. a.' AS emp_gender
FROM SA_SALES_ONLINE.SRC_SALES_ONLINE
WHERE refresh_dt > (SELECT latest_load_ts FROM BL_CL.MTA_INCREMENTAL_LOAD WHERE source_table_name = 'SRC_SALES_ONLINE' AND target_table_name = 'CE_SALES')
UNION ALL 
SELECT id, order_id, sales_date, client_id, SPLIT_PART(fullname, ' ', 1), SPLIT_PART(fullname, ' ', 2), phone_number, 'n. a.', gender, '1900-01-01',
		item_id, item_name, brand, category_id, category, subcategory_id, subcategory,
		shop_name, shop_address, 'n. a.' AS shop_online_address, 'n. a.' AS shop_email,
		province_id, province, town_id, town, district_id, district,
		payment_method, 
		'n. a.' AS delivery_address, 'n. a.' AS delivery_method,
		NULL AS delivery_fee, retail_price, quantity, sale_price, 
		emp_id, emp_first_name, emp_last_name, emp_email, emp_phone_number, emp_gender
FROM SA_SALES_OFFLINE.SRC_SALES_OFFLINE
WHERE refresh_dt > (SELECT latest_load_ts FROM BL_CL.MTA_INCREMENTAL_LOAD WHERE source_table_name = 'SRC_SALES_OFFLINE' AND target_table_name = 'CE_SALES');