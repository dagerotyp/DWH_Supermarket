CREATE SCHEMA IF NOT EXISTS BL_3NF;

-- GENERATE SEQUENCE FOR PK OF CE_TOWNS
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_TOWN_ID START 1;

CREATE TABLE IF NOT EXISTS BL_3NF.CE_TOWNS (
	town_id BIGINT PRIMARY KEY,
	province_id BIGINT NOT NULL REFERENCES BL_3NF.CE_PROVINCES (province_id),
	town_name VARCHAR(255) NOT NULL,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	town_src_id VARCHAR(255) NOT NULL,
	insert_dt DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt DATE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CONNECT SEQUENCE WITH PK OF CE_TOWNS
ALTER SEQUENCE BL_3NF.SEQ_CE_TOWN_ID OWNED BY BL_3NF.CE_TOWNS.TOWN_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_3NF.CE_TOWNS (town_id, town_name, province_id, town_src_id, source_system, source_entity, insert_dt, update_dt) 
SELECT -1 AS id,
		'n. a.',
		-1,
		'n. a.',
		'MANUAL',
		'MANUAL',
		'1990-01-01'::TIMESTAMP,
		'1990-01-01'::TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_TOWNS WHERE town_id = -1);

COMMIT;