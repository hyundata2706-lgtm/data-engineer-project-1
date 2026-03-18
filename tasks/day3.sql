/*
	TASK 1 : KHÁCH MUA 3 NGÀY LIÊN TIẾP
		- Tìm customer_id có ít nhất 3 ngày mua hàng liên tiếp
	TASK 2 : KHÁCH QUAY LẠI NGAY NGÀY HÔM SAU
		- Khách có order hôm nay và ngày hôm sau cũng có order
	TASK 3 : CHUỖI MUA HÀNG DÀI NHẤT
		- với mỗi customer, tìm số ngày liên tiếp dài nhất
	TASK 4 : RETENTION (CỰC THỰC TẾ)
		- bao nhiêu % khách quay lại sau 1 ngày
	TASK 5 : KHÁCH INACTIVE
		- > 7 ngày không mua hàng
*/
-- TASK 1
WITH t AS (
	SELECT DISTINCT customer_id, order_date FROM de_bronze.Orders
),
g AS (
	SELECT
		customer_id,
		order_date,
		DATEADD(day, -ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date),order_date) AS grp 
	FROM
		t
)
SELECT customer_id FROM g GROUP BY customer_id,grp HAVING COUNT(*) >= 3
-- TASK 2
SELECT customer_id FROM 
(
	SELECT
		customer_id, 
		order_date,
		LEAD(order_date) OVER(PARTITION BY customer_id ORDER BY order_date) AS next_order_date
	FROM 
		de_bronze.Orders
)t WHERE DATEDIFF(day, order_date,next_order_date) = 1
-- TASK 3
WITH t AS (
	SELECT DISTINCT customer_id, order_date FROM de_bronze.Orders
),g AS (
	SELECT 
		customer_id, 
		order_date,
		DATEADD(day, -ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date),order_date) AS grp
	FROM t
)
SELECT customer_id, streak AS max_streak FROM (
	SELECT DISTINCT
		customer_id, 
		COUNT(*) AS streak, 
		DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS ranked 
	FROM g GROUP BY customer_id, grp 
)t WHERE ranked=1
-- streak có thể bao gồm retention nhưng retention ko thể bao gồm streak, ở task 2 chỉ là lấy những id mà
-- DATEDIFF = 1 , còn task 3 là streak >= 1
-- TASK 4
WITH t AS (
	SELECT DISTINCT customer_id, order_date
	FROM de_bronze.Orders
),
r AS (
	SELECT DISTINCT customer_id
	FROM (
		SELECT
			customer_id,
			order_date,
			LEAD(order_date) OVER(PARTITION BY customer_id ORDER BY order_date) AS next_date
		FROM t
	)x
	WHERE DATEDIFF(day, order_date, next_date) = 1
)
SELECT 
	CAST(COUNT(*) AS FLOAT) / 
	(SELECT COUNT(DISTINCT customer_id) FROM de_bronze.Orders)
AS retention_rate
FROM r
-- TASK 5
WITH t AS (
	SELECT DISTINCT customer_id, order_date
	FROM de_bronze.Orders
)
SELECT DISTINCT customer_id
FROM (
	SELECT
		customer_id,
		order_date,
		LEAD(order_date) OVER(PARTITION BY customer_id ORDER BY order_date) AS next_date
	FROM t
)x
WHERE DATEDIFF(day, order_date, next_date) > 7


