DELETE FROM mart.f_sales
WHERE date_id IN (SELECT CAST(TO_CHAR((date_time)::DATE,'yyyymmdd') as INT) FROM staging.user_order_log);

INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT
    CAST(TO_CHAR((date_time)::DATE,'yyyymmdd') as INT) AS  date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    CASE
        WHEN status = 'refunded'
        THEN payment_amount * -1
        ELSE payment_amount
    END AS payment_amount,
    status
FROM staging.user_order_log;
