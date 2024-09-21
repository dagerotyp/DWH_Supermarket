-- PROCEDURE TO LOAD DATA TO CE_PRODUCTS
CREATE OR REPLACE PROCEDURE BL_CL.CE_PRODUCTS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_PRODUCTS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN

	WITH distinct_row AS (
		SELECT DISTINCT COALESCE(UPPER(src.item_name), 'n. a.') AS product_name,
				src.subcategory_id AS subcategory_id,
				src.category_id AS category_id,
				src.brand AS brand,
				src.item_id AS product_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
		UNION ALL
		SELECT DISTINCT COALESCE(UPPER(src.item_name), 'n. a.') AS product_name,
				src.subcategory_id AS subcategory_id,
				src.category_id AS category_id,
				src.brand AS brand,
				src.item_id AS product_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src
	),
	ce_row AS (
		SELECT dr.product_name,
				sc.product_subcategory_id,
				b.brand_id,
				dr.product_src_id,
				dr.source_system,
				dr.source_entity
		FROM distinct_row AS dr
		LEFT JOIN BL_3NF.CE_PRODUCT_SUBCATEGORIES AS sc ON sc.product_subcategory_src_id = CONCAT(dr.category_id, '_', dr.subcategory_id) AND 
															sc.source_entity  = dr.source_entity
		LEFT JOIN BL_3NF.CE_BRANDS AS b ON b.brand_src_id =	dr.brand AND 
											b.source_entity  = dr.source_entity
	)
	INSERT INTO BL_3NF.CE_PRODUCTS (product_id, product_name, brand_id, product_subcategory_id, product_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_PRODUCT_ID') AS product_id,
			ir.product_name,
			ir.brand_id,
			ir.product_subcategory_id,
			ir.product_src_id,
			ir.source_system,
			ir.source_entity
	FROM ce_row AS ir
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PRODUCTS AS curr WHERE ir.product_src_id = curr.product_src_id AND
																	ir.source_entity = curr.source_entity);
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
