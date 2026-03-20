-- TASK 1
SELECT
    customer_id,
    order_date,
    DATEFROMPARTS(YEAR(first_order_date), MONTH(first_order_date), 1) AS cohort_month,
    DATEDIFF(MONTH,
        DATEFROMPARTS(YEAR(first_order_date), MONTH(first_order_date), 1),
        DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1)
    ) AS month_index
FROM (
    SELECT
        customer_id,
        order_date,
        MIN(order_date) OVER (PARTITION BY customer_id) AS first_order_date
    FROM de_bronze.Orders
) t;
-- TASK 2
WITH t AS (
    SELECT
        customer_id,
        order_date,
        DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS order_month,
        MIN(DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1)) 
            OVER (PARTITION BY customer_id) AS cohort_month
    FROM de_bronze.Orders
)
SELECT
    cohort_month,
    DATEDIFF(MONTH, cohort_month, order_month) AS month_index,
    COUNT(DISTINCT customer_id) AS total_customers
FROM t
GROUP BY
    cohort_month,
    DATEDIFF(MONTH, cohort_month, order_month)
ORDER BY cohort_month;
-- TASK 3
SELECT DISTINCT customer_id
FROM (
    SELECT
        customer_id,
        DATEDIFF(
            DAY,
            order_date,
            LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)
        ) AS diff_days
    FROM de_bronze.Orders
) t
WHERE diff_days <= 7;
-- TASK 4
/*
load data mỗi ngày mà KHÔNG bị duplicate + có thể update dữ liệu cũ
  data mới được thêm vào orders hoặc order cũ bị update (ví dụ: amount thay đổi)
  thêm record mới
  update record cũ
  không bị duplicate
  MERGE = INSERT + UPDATE + DELETE trong 1 câu
  MERGE target_table AS t
  USING source_table AS s
  ON t.id = s.id
  WHEN MATCHED THEN 
    UPDATE SET ...
  WHEN NOT MATCHED THEN 
    INSERT (...)
    VALUES (...);
*/
IF OBJECT_ID('de_bronze.orders_clean','U') IS NOT NULL
    DROP TABLE de_bronze.orders_clean
CREATE TABLE de_bronze.orders_clean(
    order_id INT,
    customer_id INT, 
    order_date DATE,
    amount FLOAT
)
MERGE de_bronze.orders_clean AS target
USING de_bronze.Orders AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN
    UPDATE SET
        target.customer_id = source.customer_id,
        target.order_date = source.order_date,
        target.amount = source.amount

WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_date, amount)
    VALUES (source.order_id, source.customer_id, source.order_date, source.amount);
-- TASK 5 - update chỉ khi amount thay đổi
IF OBJECT_ID('de_bronze.orders_clean','U') IS NOT NULL
    DROP TABLE de_bronze.orders_clean
CREATE TABLE de_bronze.orders_clean(
    order_id INT,
    customer_id INT, 
    order_date DATE,
    amount FLOAT,
    last_updated DATE
)
MERGE de_bronze.orders_clean AS target
USING de_bronze.Orders AS source
ON target.order_id = source.order_id

WHEN MATCHED AND target.amount <> source.amount THEN
    UPDATE SET
        target.amount = source.amount,
        target.last_updated = GETDATE()

WHEN NOT MATCHED THEN
    INSERT (order_id,amount,last_updated)
    VALUES (source.order_id,source.amount, GETDATE());


