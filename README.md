# DWH_Supermarket
This is the Data Warehouse project that I've been working on during the EPAM Data Analytics engineering for Supermarket Chain using PostgreSQL

To buld DWH in PostgreSQL follow the steps included in _build_dwh_script.sql_ file.

# Project Summary:
The primary objective of this project is to create a concept data warehouse using dimensional modelling techniques (Inmon and Kimball). Identify the key stakeholders and business requirements. Based on these requirements develop a design schema for the data warehouse. In the next step data warehouse was implemented using PostgreSQL. Based on build DWH create a PowerBI report to get meaninful insights.

# Business Background
A supermarket is a self-service shop offering a wide variety of food, beverages and household products, organized into sections. This kind of store is larger and has a wider selection than earlier grocery stores. Our chain of supermarkets should provide services for both type of clients: stationary and online clients. We want to provide delivery services of our products to our customers. This kind of business is very competitive, so if you want to be successful in this field you should very responsibly approach this case and learn a lot of factors which influence on people’s choice. Knowing what type of items or brand is popular and keep track of current trends could be highly beneficial in maximizing profits. First of all, it can be done by collecting product sales information and analyzing the one using special tools. 

# Business Requirements
Client need to get basic knowledge of current state of their company which is chain of Supermarkets.
Implementing a data warehouse should help client to answer you the following questions:
-	Which items have the highest prices and have biggest margins?
-	Which items are the most profitable?
-	Which types of products are sold more often than others?
-	What types of products people usually buy on different day of the week?
-	Which products have the widest distribution of prices?
-	How does gender of a client influence their purchase behavior?
-	Does online sales are more profitable than stationary?
-	Correlate sales with seasonality
-	If there are any differences between in sales for different brands

# Datasets
Datasets comes from online and offline sales. There is also a SALES_OFFLINE_INC dataset which is used for implementation of incremental loading

**SALES_OFFLINE.CSV**
The first dataset contains the following information about sales in stationary shops.
-	ID – Unique identifier of each sold item
-	ORDER_ID – Unique identifier of given order (which can consist of multiple items sold)
-	DATE: Date of purchase of given order
-	EMP_ID: Unique identifier of employee
-	EMP_FIRST_NAME: First name of the employee	
-	EMP_LAST_NAME: Last name of the employee
-	EMP_GENDER: Gender of the employee
-	EMP_EMAIL: Email address of the employee
-	EMP_PHONE: Phone of the employee
-	PAYMENT_METHOD: Payment method of given order
-	CLIENT_ID: Unique identifier of the client
-	FULLNAME: Full name of the client
-	GENDER: Gender of the client
-	PHONE_NUMBER: Phone number of the client
-	PRODUCT_ID: Unique identifier of item
-	PRODUCT_NAME: Name of the item
-	BRAND: Brand of the item
-	SUBCATEGORY_ID: Unique identifier of subcategory
-	SUBCATEGORY: Subcategory of item
-	CATEGORY_ID: Unique identifier of category
-	CATEGORY: Category of item
-	SHOP_NAME: Name of the shop
-	PROVINCE_ID: Unique identifier of province
-	PROVINCE: City of the shop
-	TOWN_ID: Unique identifier of town
-	TOWN: Town of the shop
-	DISTRICT_ID: Unique identifier of district
-	DISTRICT: District of the shop
-	RETAIL_PRICE: Retail price of the item
-	AMOUNT: Amount of item sold
-	SALE_PRICE: Price at which given item was sold

**SALES_ONLINE.CSV**
The second dataset contains the following information about sales in online shops.
-	ID – Unique identifier of each sold item
-	ORDER_ID – Unique identifier of given order (which can consist of multiple items sold)
-	DATE: Date of purchase of given order
-	PAYMENT_METHOD: Payment method of given order
-	CLIENT_ID: Unique identifier of the client
-	FIRST_NAME: First name of the client
-	LAST_NAME: Last name of the client
-	GENDER: Gender of the client
-	EMAIL: Email address of the client
-	CLIENT_BIRTHDATE: Birthday date of the client
-	ITEM_ID: Unique identifier of item
-	ITEM_NAME: Name of the item
-	BRAND: Brand of the item
-	SUBCATEGORY_ID: Unique identifier of subcategory
-	SUBCATEGORY: Subcategory of item
-	CATEGORY_ID: Unique identifier of category
-	CATEGORY: Category of item
-	SHOP_ONLINE_ADDRESS: Name of online shop
-	SHOP_EMAIL: Email of the online shop
-	PROVINCE_ID: Unique identifier of province
-	PROVINCE: City of the delivery
-	TOWN_ID: Unique identifier of town
-	TOWN: Town of the delivery
-	DISTRICT_ID: Unique identifier of district
-	DISTRICT: District of the delivery
-	DELIVERY_ADDRESS: Delivery address of the order
-	DELIVERY_METHOD: Delivery method of the given order
-	DELIVERY_FEE: Delivery fee of the order
-	RETAIL_PRICE: Retail price of the item
-	AMOUNT: Amount of item sold
-	SALE_PRICE: Price at which given item was sold

# 3NF Schema
![DWH_Task3_BL_3NF](https://github.com/user-attachments/assets/5c18ac3d-6fe8-4be6-bd9f-2a0deaae8102)

# Dimensional Schema
![DWH_Task4_BL_DM](https://github.com/user-attachments/assets/5e2562b7-b5ed-4615-a9fc-2ed5b60d79a5)

# Logical Schema
Data Warehouse load process:
1)	Data Sources: First stage of DWH load process. Data will be extracted from two sources: Online and Offline sales. 
2)	Staging Area: Second stage of DWH load process. Data from sources is loaded to this layer. It is an intermediate layer between source and 3NF layer. Data is here to prepare for loading
3)	3NF Relational Layer: After transformation of Data at staging area it is loaded to 3NF layer. This is a normalized view of a data, where each table represents unique entity. It reduces redundancy and improve data quality
4)	Dimensional Layer: Last layer of DWH process. Data is loaded into dimensional layer where is denormalized for improving performance and efficiency for querying. This layer consist of dimensional and fact tables.
![Logical Schema of DWH](https://github.com/user-attachments/assets/397471df-79bf-49eb-bccf-1cdbc0e40fbd)

# Data Flow
Data flow for DWH:
Initially we have two sources which are link to DWH by external (foreign) tables. These external tables are loaded into staging layer which will store all data from the client. Then data is loaded to 3NF layer, where data is normalized. In the last stage data is loaded to dimensional layer where is denormalized and ready for querying.

Below data flow for each dimension is presented:
DIM_EMPLOYEES_SCD
![DIM_EMPLOYEES_SCD](https://github.com/user-attachments/assets/14accb7e-54c9-4eeb-b47e-62c93663f575)

DIM_CUSTOMERS
![DIM_CUSTOMERS](https://github.com/user-attachments/assets/76b72816-8ea5-4269-9ed6-7951f32b50bc)

DIM_PAYMENTS
![DIM_PAYMENTS](https://github.com/user-attachments/assets/9c1d945d-4756-4d54-8f73-491c1824a061)

DIM_PRODUCTS
![DIM_PRODUCTS](https://github.com/user-attachments/assets/56b2e6ca-e998-495b-af8b-e5d42180e358)

DIM_DELIVERIES
![DIM_DELIVERIES](https://github.com/user-attachments/assets/833f2b4a-9111-476d-a647-4aaa5003f4a1)

DIM_STORES
![DIM_STORES](https://github.com/user-attachments/assets/18340e81-9cfd-4678-9142-7e51362c69cc)

FCT_SALES_DD
![FCT_SALES_DD](https://github.com/user-attachments/assets/212fca90-cd6a-403a-9d1f-5a8dc5607515)






