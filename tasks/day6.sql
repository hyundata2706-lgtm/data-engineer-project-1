-- TASK 1 
/*
1. UPDATE nếu:
	order_id match 
	VÀ data thay đổi (amount hoặc status)
	update:
		amount
		status
		last_updated = GETDATE 
	INSERT nếu:
		order_id chưa tồn tại
		insert đầy đủ field + is_active = 1
3. SOFT DELETE
	Nếu:
		record có trong dwh_orders
		nhưng KHÔNG còn trong stg_orders
*/
-- TASK 1, 2 ,3 
MERGE dwh_orders AS do
USING stg_orders AS so
ON do.order_id = so.order_id
WHEN MATCHED AND (do.amount <> so.amount OR do.status <> so.status) THEN
	UPDATE SET
		do.amount = so.amount,
		do.status = do.status,
		do.last_updated = GETDATE()
WHEN NOT MATCHED
	INSERT (order_id, customer_id, amount, status,is_active, last_updated)
	VALUES (so.order_id, so.customer_id, so.amount, so.status,1, GETDATE())
WHEN NOT MATCHED BY SOURCE THEN 
	UPDATE SET
		do.is_active = 0,
		do.last_updated = GETDATE()
-- TASK 3

