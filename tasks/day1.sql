/* 
	TASK 1 : Total Revenue
		Tính tổng doanh thu của toàn hệ thống
	Task 2 : Revenue per day
		Tính doanh thu theo ngày
	Task 3 : Top 5 Customers 
		Top 5 khách hàng chi tiêu nhiều nhất
	Task 4 : Orders per day
		Mỗi ngày có bao nhiêu đơn hàng
	Task 5 : Join 
		Hiển thị customer_name, total_spent
*/
-- Task 1
SELECT SUM(amount) FROM de_bronze.Orders
-- Task 2
SELECT order_date,SUM(amount) AS revenue FROM de_bronze.Orders GROUP BY order_date
-- Task 3
SELECT customer_id, total_spent FROM (
	SELECT 
		customer_id, 
		SUM(amount) AS total_spent, 
		ROW_NUMBER() OVER(ORDER BY SUM(amount) DESC) AS rn 
	FROM de_bronze.Orders GROUP BY customer_id
)t
WHERE rn <= 5
-- Task 4
SELECT order_date, COUNT(order_id) AS total_orders FROM de_bronze.Orders GROUP BY order_date
-- Task 5
SELECT
	c.name AS customer_name, 
	SUM(o.amount) AS total_spent
FROM de_bronze.Customers 
AS c 
LEFT JOIN de_bronze.Orders AS o 
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
