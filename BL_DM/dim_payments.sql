-- GENERATE SEQUENCE FOR PK OF DIM_PAYMENTS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_PAYMENT_SURR_ID START 1;

-- CREATE DIM_PAYMENTS TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PAYMENTS (
	payment_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_PAYMENT_SURR_ID'),
	payment_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	payment_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_PAYMENTS
ALTER SEQUENCE BL_DM.SEQ_DIM_PAYMENT_SURR_ID OWNED BY BL_DM.DIM_PAYMENTS.PAYMENT_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_PAYMENTS (payment_surr_id, payment_name, payment_src_id, source_system, source_entity)
SELECT -1 AS payment_surr_id,
		'n. a.' AS payment_name,
		'n. a.' AS payment_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PAYMENTS AS curr WHERE curr.payment_surr_id = -1);
															
COMMIT;