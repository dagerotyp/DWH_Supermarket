-- PROCEDURE TO LOAD DATA TO DIM PRODUCTS TABLE
CREATE OR REPLACE PROCEDURE BL_CL.DIM_PRODUCTS_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.DIM_PRODUCTS_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	-- Data load from BL_3NF Layer
	INSERT INTO BL_DM.DIM_PRODUCTS (product_surr_id, brand_id, brand_name, product_subcategory_id, product_subcategory_name, product_category_id,
	product_category_name, product_name, product_src_id, source_system, source_entity)
	SELECT nextval('BL_DM.SEQ_DIM_PRODUCT_SURR_ID'),
			COALESCE(b.brand_id, -1),
			b.brand_name,
			p.product_subcategory_id,
			sc.product_subcategory_name,
			c.product_category_id,
			c.product_category_name,
			p.product_name,
			p.product_id::VARCHAR AS product_src_id,
			'BL_3NF' AS source_system,
			'CE_EMPLOYEES_SCD'  AS source_entity
	FROM BL_3NF.ce_products AS p
	LEFT JOIN BL_3NF.ce_product_subcategories AS sc ON p.product_subcategory_id = sc.product_subcategory_id
	LEFT JOIN BL_3NF.ce_product_categories AS c ON c.product_category_id = sc.product_category_id
	LEFT JOIN BL_3NF.ce_brands AS b ON b.brand_id = p.brand_id
	WHERE p.product_id <> -1 AND NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PRODUCTS AS curr WHERE curr.product_src_id = p.product_id::VARCHAR AND
																	curr.source_entity = 'CE_EMPLOYEES_SCD');
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
