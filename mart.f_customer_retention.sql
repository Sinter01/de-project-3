CREATE TABLE mart.f_customer_retention (
    new_customers_count INT8 NOT NULL,
    returning_customers_count INT8 NOT NULL,
    refunded_customer_count INT8 NOT NULL,
    period_name VARCHAR(100) NOT NULL,
    period_id BPCHAR(10) NOT NULL,
    category_id VARCHAR(100) NOT NULL,
    new_customers_revenue NUMERIC(10, 2) NOT NULL,
    returning_customers_revenue NUMERIC(10, 2) NOT NULL,
    customers_refunded INT8 NOT NULL
);

WITH weeks_in_f_sales AS 
(
	SELECT
		week_of_year_iso AS week
	FROM mart.f_sales f_s
		LEFT JOIN mart.d_calendar d_c ON f_s.date_id = d_c.date_id
)

DELETE FROM mart.f_customer_retention
WHERE period_id IN (SELECT week FROM weeks_in_f_sales);

INSERT INTO mart.f_customer_retention

WITH user_orders AS
(
	SELECT
		week_of_year_iso AS week,
		customer_id,
		COUNT(DISTINCT id) AS orders,
		SUM(payment_amount) AS payment_amount
	FROM mart.f_sales f_s
		LEFT JOIN mart.d_calendar d_c ON f_s.date_id = d_c.date_id
	GROUP BY 1, 2
),

user_refunders AS 
(
	SELECT
		week_of_year_iso AS week,
		customer_id,
		COUNT(DISTINCT id) AS refunders
	FROM mart.f_sales f_s
		LEFT JOIN mart.d_calendar d_c ON f_s.date_id = d_c.date_id
	WHERE f_s.status = 'refunded'
	GROUP BY 1, 2
)

SELECT 
	ncc.week AS period_id,
	'weekly' AS period_name,
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
FROM
(
	SELECT
		week,
		COUNT(DISTINCT customer_id) AS new_customers_count
	FROM user_orders
	WHERE orders = 1
	GROUP BY 1
) AS ncc
LEFT JOIN 
(
	SELECT
		week,
		COUNT(DISTINCT customer_id) AS returning_customers_count
	FROM user_orders
	WHERE orders > 1
	GROUP BY 1
) AS rcc ON ncc.week = rcc.week
LEFT JOIN
(
	SELECT
		week,
		COUNT(DISTINCT customer_id) AS refunded_customer_count
	FROM user_refunders
	GROUP BY 1
) AS refcc ON ncc.week = refcc.week
LEFT JOIN 
(
	SELECT
		week,
		SUM(payment_amount) AS new_customers_revenue
	FROM user_orders
	WHERE orders = 1
	GROUP BY 1
) AS ncr ON ncc.week = ncr.week
LEFT JOIN 
(
	SELECT
		week,
		SUM(payment_amount) AS returning_customers_revenue
	FROM user_orders
	WHERE orders > 1
	GROUP BY 1
) AS rcr ON ncc.week = rcr.week
LEFT JOIN 
(
	SELECT
		week,
		SUM(refunders) AS customers_refunded
	FROM user_refunders
	GROUP BY 1
) AS cr ON ncc.week = cr.week;
