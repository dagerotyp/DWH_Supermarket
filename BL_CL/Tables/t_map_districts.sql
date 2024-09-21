-- GENERATE SEQUENCE FOR PK OF T MAP DISTRICTS
CREATE SEQUENCE IF NOT EXISTS BL_CL.T_MAP_DISTRICTS_ID_SEQ START 1; 

-- CREATE TABLE FOR DEDUPLICATION OF DISTRICTS
CREATE TABLE IF NOT EXISTS BL_CL.T_MAP_DISTRICTS (
	district_id BIGINT NOT NULL DEFAULT nextval('BL_CL.T_MAP_DISTRICTS_ID_SEQ'),
	district_name VARCHAR(255) NOT NULL,
	province_src_name VARCHAR(255) NOT NULL,
	town_src_name VARCHAR(255) NOT NULL,
	district_src_name VARCHAR(255) NOT NULL,
	district_src_id VARCHAR(255),
	source_system VARCHAR(255),
	source_entity VARCHAR(255)
);

-- CONNECT SEQUENCE WITH PK OF T MAP DISTRICTS
ALTER SEQUENCE BL_CL.T_MAP_DISTRICTS_ID_SEQ OWNED BY BL_CL.T_MAP_DISTRICTS.DISTRICT_ID;