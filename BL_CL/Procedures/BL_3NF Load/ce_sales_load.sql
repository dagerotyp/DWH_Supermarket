-- PROCEDURE TO LOAD DATA TO CE_SALES
CREATE OR REPLACE PROCEDURE BL_CL.CE_SALES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_SALES_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
	src_online_latest_load TIMESTAMP := (SELECT latest_load_ts FROM BL_CL.MTA_INCREMENTAL_LOAD WHERE source_table_name = 'SRC_SALES_ONLINE' AND target_table_name = 'CE_SALES');
	src_offline_latest_load TIMESTAMP := (SELECT latest_load_ts FROM BL_CL.MTA_INCREMENTAL_LOAD WHERE source_table_name = 'SRC_SALES_OFFLINE' AND target_table_name = 'CE_SALES');
BEGIN
	WITH distinct_row AS (
		SELECT src.order_id AS order_id,
				src.sales_date AS sales_date,
				src.item_id AS product_src_id,
				src.client_id AS customer_src_id,
				src.shop_name AS store_src_id,
				src.payment_method AS payment_method_src_id,
				NULL AS delivery_src_id,
				src.emp_id AS emp_src_id,
				NULL AS delivery_fee,
				REPLACE(src.retail_price, ',', '.')::NUMERIC(10,2) AS retail_price, -- FIX RETAIL_PRICE IN DATASET
				src.quantity::INT AS quantity,
				REPLACE(src.sale_price, ',', '.')::NUMERIC(10,2) AS sale_price, -- FIX SALE_PRICE IN DATASET
				src.id AS sale_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
		WHERE src.refresh_dt > src_offline_latest_load 
		UNION ALL
		SELECT src.order_id AS order_id,
				src.sales_date AS sales_date,
				src.item_id AS product_src_id,
				src.client_id AS customer_src_id,
				src.shop_email AS store_src_id,
				src.payment_method AS payment_method_src_id,
				src.order_id as delivery_src_id,
				NULL AS emp_src_id,
				REPLACE(src.delivery_fee, ',', '.')::NUMERIC(10,2) AS delivery_fee,
				REPLACE(src.retail_price, ',', '.')::NUMERIC(10,2) AS retail_price,
				src.quantity::INT AS quantity,
				REPLACE(src.sale_price, ',', '.')::NUMERIC(10,2) AS sale_price,
				src.id AS sale_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src 
		WHERE src.refresh_dt > src_online_latest_load 
	),
	insert_row AS (
		SELECT dr.order_id,
				dr.sales_date,
				c.customer_id,
				pm.payment_method_id,
				s.store_id,
				p.product_id,
				e.employee_id,
				d.delivery_id,
				dr.delivery_fee,
				dr.retail_price,
				dr.quantity,
				dr.sale_price,
				dr.sale_src_id,
				dr.source_system,
				dr.source_entity
		FROM distinct_row AS dr
		LEFT JOIN BL_3NF.ce_customers AS c ON c.customer_src_id = dr.customer_src_id AND 
												c.source_entity = dr.source_entity
		LEFT JOIN BL_3NF.ce_payment_methods AS pm ON pm.payment_method_src_id = dr.payment_method_src_id AND 
												pm.source_entity = dr.source_entity
		LEFT JOIN BL_3NF.ce_stores AS s ON s.store_src_id = dr.store_src_id AND 
												s.source_entity = dr.source_entity
		LEFT JOIN BL_3NF.ce_products AS p ON p.product_src_id = dr.product_src_id AND 
												p.source_entity = dr.source_entity	
		LEFT JOIN BL_3NF.ce_employees_scd AS e ON e.employee_src_id = dr.emp_src_id AND 
												e.source_entity = dr.source_entity	
		LEFT JOIN BL_3NF.ce_deliveries AS d ON d.delivery_src_id = dr.delivery_src_id AND 
												d.source_entity = dr.source_entity
		WHERE e.is_active IS NULL OR e.is_active = TRUE
	)
	INSERT INTO BL_3NF.CE_SALES (sale_id, order_id, customer_id, payment_method_id, delivery_id, store_id, product_id, employee_id, event_dt, delivery_fee,
								retail_price, quantity, sale_price, sale_src_id, source_system, source_entity)	
	SELECT nextval('BL_3NF.SEQ_CE_SALE_ID') AS sale_id,
			ir.order_id::BIGINT AS order_id,
			COALESCE(ir.customer_id, -1), 
			COALESCE(ir.payment_method_id, -1),
			COALESCE(ir.delivery_id, -1),
			COALESCE(ir.store_id, -1),
			COALESCE(ir.product_id, -1),
			COALESCE(ir.employee_id, -1),
			ir.sales_date::DATE AS event_dt,
			ir.delivery_fee AS delivery_fee,
			ir.retail_price,
			ir.quantity,
			ir.sale_price,
			ir.sale_src_id,
			ir.source_system,
			ir.source_entity
	FROM insert_row AS ir;
--	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.ce_sales AS curr WHERE ir.sale_src_id = curr.sale_src_id AND
--																	ir.source_entity = curr.source_entity);
-- LOGGING
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		n_row := ROW_COUNT;
	
-- ADD INFORMATION ABOUT INCREMENTAL LOAD ONLY IF NEW DATA WAS INSERTED
	IF n_row > 0 THEN
		CALL BL_CL.INCREMENTAL_LOAD_UPDATE('SRC_SALES_ONLINE'::TEXT, 'CE_SALES'::TEXT, 'CE_SALES_LOAD'::TEXT, NOW()::TIMESTAMP);
		CALL BL_CL.INCREMENTAL_LOAD_UPDATE('SRC_SALES_OFFLINE'::TEXT, 'CE_SALES'::TEXT, 'CE_SALES_LOAD'::TEXT, NOW()::TIMESTAMP);
	END IF;

	CALL BL_CL.log_procedure(proc_name, n_row);

	RAISE NOTICE 'Data loaded sucessfully. % Rows inserted', n_row;
																
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;
			RAISE NOTICE 'Data insertion failed';
				
			GET STACKED DIAGNOSTICS 
				context := PG_EXCEPTION_CONTEXT,
				err_detail := PG_EXCEPTION_DETAIL;
				
			CALL BL_CL.log_procedure(proc_name, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
			RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;															
END;
$$;
