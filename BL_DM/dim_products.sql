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