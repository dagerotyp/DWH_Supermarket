CREATE SCHEMA IF NOT EXISTS BL_3NF;

-- GENERATE SEQUENCE FOR PK OF CE_PRODUCT_CATEGORIES
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_PRODUCT_SUBCATEGORY_ID START 1;

CREATE TABLE IF NOT EXISTS BL_3NF.CE_PRODUCT_SUBCATEGORIES (
	product_subcategory_id BIGINT PRIMARY KEY,
	product_category_id BIGINT NOT NULL REFERENCES BL_3NF.CE_PRODUCT_CATEGORIES (product_category_id),
	product_subcategory_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	product_subcategory_src_id VARCHAR(255) NOT NULL,
	insert_dt DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt DATE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF CE_PRODUCT_CATEGORIES
ALTER SEQUENCE BL_3NF.SEQ_CE_PRODUCT_SUBCATEGORY_ID OWNED BY BL_3NF.CE_PRODUCT_SUBCATEGORIES.PRODUCT_SUBCATEGORY_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_3NF.CE_PRODUCT_SUBCATEGORIES(product_subcategory_id, product_subcategory_name, product_category_id, product_subcategory_src_id, source_system, source_entity, insert_dt, update_dt) 
SELECT -1 AS id,
		'n. a.',
		-1,
		'n. a.',
		'MANUAL',
		'MANUAL',
		'1990-01-01'::TIMESTAMP,
		'1990-01-01'::TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PRODUCT_SUBCATEGORIES WHERE product_subcategory_id = -1);

COMMIT;
