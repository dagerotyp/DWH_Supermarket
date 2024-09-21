-- PROCEDURE TO LOAD DATA TO CE_PRODUCT_SUBCATEGORIES
CREATE OR REPLACE PROCEDURE BL_CL.CE_PRODUCT_SUBCATEGORIES_LOAD()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_name TEXT = 'BL_CL.CE_PRODUCT_SUBCATEGORIES_LOAD';
	context TEXT; n_row INT = 0; err_detail TEXT;
BEGIN
	
	WITH distinct_row AS (
		SELECT DISTINCT COALESCE(UPPER(src.subcategory), 'n. a.') AS product_subcategory_name,
				src.category_id AS category_id,
				CONCAT(src.category_id, '_', src.subcategory_id) AS product_subcategory_src_id,
				'SA_SALES_OFFLINE' AS source_system,
				'SRC_SALES_OFFLINE' AS source_entity
		FROM SA_SALES_OFFLINE.src_sales_offline AS src
		UNION ALL
		SELECT DISTINCT COALESCE(UPPER(src.subcategory), 'n. a.') AS product_subcategory_name,
				src.category_id AS category_id,
				CONCAT(src.category_id, '_', src.subcategory_id) AS product_subcategory_src_id,
				'SA_SALES_ONLINE' AS source_system,
				'SRC_SALES_ONLINE' AS source_entity
		FROM SA_SALES_ONLINE.src_sales_online AS src
	),
	ce_row AS (
		SELECT dr.product_subcategory_name,
				p.product_category_id,
				dr.product_subcategory_src_id,
				dr.source_system,
				dr.source_entity
		FROM distinct_row AS dr
		LEFT JOIN BL_3NF.CE_PRODUCT_CATEGORIES AS p ON p.product_category_src_id = dr.category_id AND
												p.source_entity = dr.source_entity
	)
	INSERT INTO BL_3NF.CE_PRODUCT_SUBCATEGORIES (product_subcategory_id, product_subcategory_name, product_category_id, product_subcategory_src_id, source_system, source_entity)
	SELECT nextval('BL_3NF.SEQ_CE_PRODUCT_SUBCATEGORY_ID') AS product_subcategory_id,
			ir.product_subcategory_name,
			ir.product_category_id,
			ir.product_subcategory_src_id,
			ir.source_system,
			ir.source_entity
	FROM ce_row AS ir 
	WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PRODUCT_SUBCATEGORIES AS curr WHERE ir.product_subcategory_src_id = curr.product_subcategory_src_id AND
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
