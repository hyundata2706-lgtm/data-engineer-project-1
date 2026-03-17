BULK INSERT de_bronze.Customers
		FROM 'E:\data engineer project\datasets\customers.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			TABLOCK
		);
BULK INSERT de_bronze.Orders
	FROM 'E:\data engineer project\datasets\orders.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		CODEPAGE = '65001',
		TABLOCK
	);
BULK INSERT de_bronze.Products
	FROM 'E:\data engineer project\datasets\products.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		CODEPAGE = '65001',
		TABLOCK
	);
BULK INSERT de_bronze.Order_Items
	FROM 'E:\data engineer project\datasets\order_items.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		CODEPAGE = '65001',
		TABLOCK
	)
