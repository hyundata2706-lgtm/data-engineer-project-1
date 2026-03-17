USE DE3172026
IF OBJECT_ID('de_bronze.Customers','U') IS NOT NULL
	DROP TABLE de_bronze.Customers
CREATE TABLE de_bronze.Customers(
	customer_id INT,
	name VARCHAR(50),
	signup_date DATE
)
IF OBJECT_ID('de_bronze.Orders','U') IS NOT NULL
	DROP TABLE de_bronze.Orders
CREATE TABLE de_bronze.Orders(
	order_id INT,
	customer_id INT,
	order_date DATE,
	amount FLOAT
)
IF OBJECT_ID('de_bronze.Products','U') IS NOT NULL
	DROP TABLE de_bronze.Products
CREATE TABLE de_bronze.Products(
	product_id INT, 
	name VARCHAR(50),
	price FLOAT
)
IF OBJECT_ID('de_bronze.Order_Items','U') IS NOT NULL
	DROP TABLE de_bronze.Order_Items
CREATE TABLE de_bronze.Order_Items(
	order_id INT,
	product_id INT,
	quantity INT
)
