CREATE SCHEMA IF NOT EXISTS BL_3NF;

-- GENERATE SEQUENCE FOR PK OF CE_PRODUCT_CATEGORIES
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_PRODUCT_CATEGORY_ID START 1;

CREATE TABLE IF NOT EXISTS BL_3NF.CE_PRODUCT_CATEGORIES (
	product_category_id BIGINT PRIMARY KEY,
	product_category_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	product_category_src_id VARCHAR(255) NOT NULL,
	insert_dt DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt DATE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF CE_PRODUCT_CATEGORIES
ALTER SEQUENCE BL_3NF.SEQ_CE_PRODUCT_CATEGORY_ID OWNED BY BL_3NF.CE_PRODUCT_CATEGORIES.PRODUCT_CATEGORY_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_3NF.CE_PRODUCT_CATEGORIES(product_category_id, product_category_name, product_category_src_id, source_system, source_entity, insert_dt, update_dt) 
SELECT -1 AS id,
		'n. a.',
		'n. a.',
		'MANUAL',
		'MANUAL',
		'1990-01-01'::TIMESTAMP,
		'1990-01-01'::TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PRODUCT_CATEGORIES WHERE product_category_id = -1);

COMMIT;