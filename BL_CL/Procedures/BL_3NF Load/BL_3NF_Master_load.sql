-- MASTER PROCEDURE TO LOAD DATA TO BL_3NF TABLES.
CREATE OR REPLACE PROCEDURE BL_CL.BL_3NF_LOAD_MASTER()
LANGUAGE plpgsql
AS $$
BEGIN
	RAISE INFO 'LOADING CE_PROVINCES';
	CALL BL_CL.CE_PROVINCES_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_TOWNS';
	CALL BL_CL.CE_TOWNS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_DISTRICTS';
	CALL BL_CL.CE_DISTRICTS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_ADDRESSES';
	CALL BL_CL.CE_ADDRESSES_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_PRODUCT_CATEGORIES';
	CALL BL_CL.CE_PRODUCT_CATEGORIES_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_PRODUCT_SUBCATEGORIES';
	CALL BL_CL.CE_PRODUCT_SUBCATEGORIES_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_BRANDS';
	CALL BL_CL.CE_BRANDS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_PRODUCTS';
	CALL BL_CL.CE_PRODUCTS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_CUSTOMERS';
	CALL BL_CL.CE_CUSTOMERS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_PAYMENT_METHODS';
	CALL BL_CL.CE_PAYMENT_METHODS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_DELIVERY_METHODS';
	CALL BL_CL.CE_DELIVERY_METHODS_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_DELIVERIES';
	CALL BL_CL.CE_DELIVERIES_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_STORES';
	CALL BL_CL.CE_STORES_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_EMPLOYEES_SCD';
	CALL BL_CL.CE_EMPLOYEES_SCD_LOAD();
	COMMIT;
	RAISE INFO 'LOADING CE_SALES';
	CALL BL_CL.CE_SALES_LOAD();
	RAISE INFO 'FINISHED LOADING OF 3NF LAYER';
	COMMIT;
END;
$$;
