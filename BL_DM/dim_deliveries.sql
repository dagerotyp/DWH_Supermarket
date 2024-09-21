-- GENERATE SEQUENCE FOR PK OF DIM_DELIVERIES
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_DELIVERY_SURR_ID START 1;

-- CREATE DIM_DELIVERIES TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_DELIVERIES (
	delivery_surr_id BIGINT PRIMARY KEY DEFAULT nextval('BL_DM.SEQ_DIM_DELIVERY_SURR_ID'),
	delivery_method_id BIGINT NOT NULL,
	delivery_method_name VARCHAR(255) NOT NULL,
	address_id BIGINT NOT NULL,
	address VARCHAR(255) NOT NULL,
	district_id BIGINT NOT NULL,
	district_name VARCHAR(255) NOT NULL,
	town_id BIGINT NOT NULL,
	town_name VARCHAR(255) NOT NULL,
	province_id BIGINT NOT NULL,
	province_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	delivery_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF DIM_DELIVERIES
ALTER SEQUENCE BL_DM.SEQ_DIM_DELIVERY_SURR_ID OWNED BY BL_DM.DIM_DELIVERIES.DELIVERY_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_DELIVERIES (delivery_surr_id, delivery_method_id, delivery_method_name, address_id, address, district_id, district_name,
								town_id, town_name, province_id, province_name, delivery_src_id, source_system, source_entity)
SELECT -1 AS delivery_surr_id,
		-1 AS delivery_method_id,
		'n. a.' AS delivery_method_name,
		-1 AS address_id,
		'n. a.' AS address,
		-1 AS district_id,
		'n. a.' AS district_name,
		-1 AS town_id,
		'n. a.' AS town_name,
		-1 AS province_id,
		'n. a.' AS province_name,
		'n. a.' AS delivery_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_DELIVERIES AS curr WHERE curr.delivery_surr_id = -1);
															
COMMIT;