/*
	TASK 1 - TOP CUSTOMER MỖI NGÀY
		với mỗi order_date, tìm khách chi nhiều nhất trong ngày
	TASK 2 - TOP 2 CUSTOMER MỖI NGÀY
		mỗi ngày lấy top 2 khách chi nhiều nhất
	TASK 3 - TOP PRODUCT bán chạy nhất
		tìm sản phẩm bán nhiều nhất ( theo quantity)
	TASK 4 - TOP PRODUCT mỗi ngày
		mỗi ngày tìm sản phẩm bán chạy nhất
	TASK 5 - TOP CUSTOMER mỗi tháng
		mỗi tháng tìm khách chi nhiều nhất

*/
-- TASK 1
SELECT order_date, customer_id, total_spent FROM (
	SELECT 
		order_date, 
		customer_id,
		SUM(amount) AS total_spent,
		DENSE_RANK() OVER(PARTITION BY order_date ORDER BY SUM(amount) DESC) AS rn 
	FROM de_bronze.Orders
	GROUP BY order_date, customer_id
)t 
WHERE rn=1
-- TASK 2
SELECT order_date, customer_id, total_spent FROM (
	SELECT 
		order_date, 
		customer_id,
		SUM(amount) AS total_spent,
		DENSE_RANK() OVER(PARTITION BY order_date ORDER BY SUM(amount) DESC) AS rn 
	FROM de_bronze.Orders
	GROUP BY order_date, customer_id
)t 
WHERE rn<=2
-- TASK 3
SELECT TOP 1 
	product_id, 
	SUM(quantity) AS total_quantity 
FROM de_bronze.Order_Items 
GROUP BY product_id 
ORDER BY SUM(quantity) DESC
-- TASK 4
SELECT order_date, product_id, total_quantity FROM
(
	SELECT 
		o.order_date AS order_date,
		oi.product_id AS product_id, 
		SUM(oi.quantity) AS total_quantity,
		DENSE_RANK() OVER(PARTITION BY order_date ORDER BY SUM(oi.quantity) DESC) AS rn
	FROM de_bronze.Order_Items AS oi 
	LEFT JOIN de_bronze.Orders AS o 
	ON oi.order_id = o.order_id
	GROUP BY o.order_date,oi.product_id
)t
WHERE rn=1
-- TASK 5
SELECT month, customer_id, total_spent FROM (
	SELECT  
		FORMAT(order_date,'yyyy-MM')  AS month, 
		customer_id, 
		SUM(amount) AS total_spent,
		RANK() OVER(PARTITION BY FORMAT(order_date,'yyyy-MM') ORDER BY SUM(amount) DESC) AS rn
	FROM de_bronze.Orders
	GROUP BY FORMAT(order_date,'yyyy-MM'), customer_id
)t WHERE rn=1




