CREATE TABLE IF NOT EXISTS BL_DM.DIM_TIME_DAY ( -- Currently WITHOUT SCHEMA name
	date_id DATE PRIMARY KEY,
	day_of_year_number INT,
	day_of_month_number INT,
	day_of_week_number INT,
	day_name VARCHAR(9),
	weekend_flag BOOL,
	week_of_year_number INT,
	month_value INT,
	month_name VARCHAR(9),
	year_month VARCHAR(7),
	quarter_value INT,
	year_quarter VARCHAR(7),
	year_value INT
);

INSERT INTO BL_DM.DIM_TIME_DAY (date_id, 
						day_of_year_number,
						day_of_month_number, 
						day_of_week_number,
						day_name, 
						weekend_flag,
						week_of_year_number, 
						month_value, 
						month_name,
						year_month,
						quarter_value, 
						year_quarter,
						year_value)		
SELECT DATE(dd),
		EXTRACT('doy' FROM dd)::INT, -- Number OF DAY IN YEAR (1-365)
		TO_CHAR(dd, 'DD')::INT, -- Number OF DAY IN MONTH (1-31)
		TO_CHAR(dd, 'ID')::INT, -- Number OF DAY IN week (1-7)
		TO_CHAR(dd, 'DAY'),		-- Name OF a DAY (Monday, Thursday, ...)
		CASE WHEN TO_CHAR(dd, 'ID') IN ('6', '7') THEN TRUE
			ELSE FALSE END,			-- Flag whether DAY IS a weekend (1) OR NOT (0)
		TO_CHAR(dd, 'IW')::INT, -- Number OF week IN year
		TO_CHAR(dd, 'MM')::INT, -- Number OF MONTH IN year
		TO_CHAR(dd, 'MONTH'),   -- Name OF a month
		TO_CHAR(dd, 'YYYY-MM'), -- Year and month formated AS YYYY-MM
		TO_CHAR(dd, 'Q')::INT,  -- Number OF quarter IN year
		TO_CHAR(dd, 'YYYY-Q'), 	-- Year and quarter formated AS YYYY-Q
		TO_CHAR(dd, 'YYYY')::INT -- Number OF year
FROM generate_series
        ( '2020-01-01'::TIMESTAMP
        , '2040-12-31'::TIMESTAMP
        , '1 day'::interval) dd
ON CONFLICT DO NOTHING;
COMMIT;

	