-- PROCEDURE TO LOAD DATA TO FCT SALES DD TABLE
CREATE OR REPLACE PROCEDURE BL_CL.FCT_SALES_DD_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.FCT_SALES_DD_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
	year_month TEXT;
BEGIN
	-- CREATE NEW PARTITION IF IT DIDNT EXISTED
	FOR year_month IN (SELECT DISTINCT TO_CHAR(event_dt, 'YYYY_MM') FROM BL_3NF.CE_SALES) LOOP
		RAISE NOTICE 'BUILIDING PARTITION FOR: %', year_month;
		EXECUTE
			'CREATE TABLE IF NOT EXISTS BL_DM.FCT_SALES_DD_'|| year_month ||' 
			PARTITION OF BL_DM.FCT_SALES_DD
			FOR VALUES FROM ('||quote_literal(CONCAT(year_month,'_01'))||'::DATE) TO 
			('||quote_literal(CONCAT(year_month,'_01'))||'::DATE + INTERVAL '|| quote_literal('1 Month') ||');';
	END LOOP;

	WITH insert_row AS (
		SELECT s.order_id,
				s.event_dt,
				COALESCE(py.payment_surr_id, -1) AS payment_method_id,
				COALESCE(d.delivery_surr_id, -1) AS delivery_id,
				COALESCE(c.customer_surr_id, -1) AS customer_id,
				COALESCE(st.store_surr_id, -1) AS store_id,
				COALESCE(p.product_surr_id, -1) AS product_id,
				COALESCE(e.employee_surr_id, -1) AS employee_id,
				s.delivery_fee,
				s.retail_price,
				s.quantity,
				s.sale_price,
				s.sale_price * s.quantity AS fct_total_price,
				CASE WHEN s.retail_price != 0 THEN ROUND((s.retail_price - s.sale_price)/s.retail_price, 2)
					ELSE 0 END AS fct_discount,
				s.sale_id::VARCHAR AS sale_src_id,
				'BL_3NF' AS source_system,
				'CE_SALES' AS source_entity
		FROM BL_3NF.CE_SALES AS s
		LEFT JOIN BL_DM.DIM_CUSTOMERS AS c ON c.customer_src_id = s.customer_id::VARCHAR
		LEFT JOIN BL_DM.DIM_PRODUCTS AS p ON p.product_src_id = s.product_id::VARCHAR
		LEFT JOIN BL_DM.DIM_EMPLOYEES_SCD AS e ON e.employee_src_id = s.employee_id::VARCHAR
		LEFT JOIN BL_DM.DIM_STORES AS st ON st.store_src_id = s.store_id::VARCHAR
		LEFT JOIN BL_DM.DIM_PAYMENTS AS py ON py.payment_src_id = s.payment_method_id::VARCHAR
		LEFT JOIN BL_DM.DIM_DELIVERIES AS d ON d.delivery_src_id = s.delivery_id::VARCHAR
		WHERE e.is_active IS NULL OR e.is_active = TRUE
		EXCEPT 
		SELECT fct.order_id, fct.event_dt, fct.payment_method_id, fct.delivery_id, fct.customer_id, fct.store_id, fct.product_id, fct.employee_id, 
				fct.delivery_fee, fct.retail_price, fct.quantity, fct.sale_price, fct.fct_total_price, fct.fct_discount, fct.sale_src_id, fct.source_system, fct.source_entity
		FROM BL_DM.FCT_SALES_DD AS fct
	)
	INSERT INTO BL_DM.FCT_SALES_DD (sale_surr_id, order_id, event_dt, payment_method_id, delivery_id, customer_id, store_id, product_id, employee_id,
									delivery_fee, retail_price, quantity, sale_price, fct_total_price, fct_discount, sale_src_id, source_system, source_entity)
	SELECT nextval('BL_DM.SEQ_FCT_SALES_DD_SURR_ID'),
			ir.order_id,
			ir.event_dt,
			ir.payment_method_id,
			ir.delivery_id,
			ir.customer_id,
			ir.store_id,
			ir.product_id,
			ir.employee_id,
			ir.delivery_fee,
			ir.retail_price,
			ir.quantity,
			ir.sale_price,
			ir.fct_total_price,
			ir.fct_discount,
			ir.sale_src_id,
			ir.source_system,
			ir.source_entity
	FROM insert_row AS ir;

-- LOGGING
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		n_row := ROW_COUNT;
	
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
