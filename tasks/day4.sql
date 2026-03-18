/*
Trong mỗi tháng, tìm TOP 2 khách hàng có tổng chi tiêu cao nhất,
NHƯNG CHỈ GIỮ những khách:
	Có ít nhất 3 ngày mua hàng liên tiếp
	Và có ít nhất 1 lần mua hàng vào ngày hôm sau (D và D+1)
*/

WITH base AS (
	SELECT DISTINCT customer_id, order_date FROM de_bronze.Orders
), consecutive AS (
	SELECT customer_id FROM (
		SELECT 
			customer_id,
			order_date,
			DATEDIFF(day, -ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date),order_date) AS grp
		FROM base
	)t
	GROUP BY customer_id, grp
	HAVING count(*) >=3
), retention AS (
	SELECT customer_id FROM (
		SELECT
			customer_id,
			order_date,
			LEAD(order_date, 1,null) OVER(PARTITION BY customer_id ORDER BY order_date) AS next_order_date
		FROM base
	)x
	WHERE DATEDIFF(day,order_date, next_order_date)=1
),valid_customer AS (
	SELECT customer_id
	FROM consecutive
	INTERSECT
	SELECT customer_id
	FROM retention
),spending AS (
	SELECT
		FORMAT(order_date,'yyyy-MM') AS month,
		customer_id, 
		SUM(amount) AS total_spent
	FROM de_bronze.Orders
	WHERE customer_id IN (SELECT customer_id FROM valid_customer)
	GROUP BY customer_id, FORMAT(order_date,'yyyy-MM')
)

SELECT * FROM (
	SELECT *,
		DENSE_RANK() OVER(PARTITION BY month ORDER BY total_spent DESC) AS rnk
	FROM spending
)x
WHERE rnk<=2
