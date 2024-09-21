-- GENERATE SEQUENCE FOR PK OF DIM_EMPLOYEES_SCD
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID START 1;

-- CREATE DIM_EMPLOYEES_SCD TABLE
CREATE TABLE IF NOT EXISTS BL_DM.DIM_EMPLOYEES_SCD (
	employee_surr_id BIGINT DEFAULT nextval('BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID'),
	first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	phone_number VARCHAR(255) NOT NULL,
	gender VARCHAR(255)NOT NULL,
	start_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	end_dt TIMESTAMP NOT NULL DEFAULT '9999-12-31'::TIMESTAMP,
	is_active BOOL NOT NULL DEFAULT TRUE,
	source_system VARCHAR(255) NOT NULL,
	source_entity VARCHAR(255) NOT NULL,
	employee_src_id VARCHAR(255) NOT NULL,
	insert_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	update_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (employee_surr_id, start_dt)
);

-- CONNECT SEQUENCE WITH PK OF DIM_DELIVERIES
ALTER SEQUENCE BL_DM.SEQ_DIM_EMPLOYEE_SURR_ID OWNED BY BL_DM.DIM_EMPLOYEES_SCD.EMPLOYEE_SURR_ID;

-- INSERT DEFAULT ROW
INSERT INTO BL_DM.DIM_EMPLOYEES_SCD (employee_surr_id, first_name, last_name, email, phone_number, gender, employee_src_id, source_system, source_entity)
SELECT -1 AS employee_surr_id,
		'n. a.' AS first_name,
		'n. a.' AS last_name,
		'n. a.' AS email,
		'n. a.' AS phone_number,
		'n. a.' AS gender,
		'-1' AS employee_src_id,
		'MANUAL' AS source_system,
		'MANUAL' AS source_entity
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_EMPLOYEES_SCD WHERE employee_surr_id = -1);

COMMIT;