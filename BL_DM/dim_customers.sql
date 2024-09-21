-- GENERATE SEQUENCE FOR PK OF DIM_CUSTOMERS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_CUSTOMER_SURR_ID START 1;

-- CREATE DIM_CUSTOMERS TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_CUSTOMERS (
	customer_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_CUSTOMER_SURR_ID'),
	first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	phone_number VARCHAR(255) NOT NULL,
	gender VARCHAR(255) NOT NULL,
	birthdate_dt DATE NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	customer_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_CUSTOMERS
ALTER SEQUENCE BL_DM.SEQ_DIM_CUSTOMER_SURR_ID OWNED BY BL_DM.DIM_CUSTOMERS.CUSTOMER_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_CUSTOMERS (customer_surr_id, first_name, last_name, email, phone_number, gender, birthdate_dt, customer_src_id, source_system, source_entity)
SELECT -1 AS customer_surr_id,
		'n. a.' AS first_name,
		'n. a.' AS last_name,
		'n. a.' AS email,
		'n. a.' AS phone_number,
		'n. a.' AS gender,
		'1990-01-01'::DATE AS birthdate_dt,
		'n. a.' AS customer_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_CUSTOMERS AS curr WHERE curr.customer_surr_id = -1);
															
COMMIT;