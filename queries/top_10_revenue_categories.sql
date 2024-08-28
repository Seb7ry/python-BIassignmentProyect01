-- TODO: This query will return a table with the top 10 revenue categories in 
-- English, the number of orders and their total revenue. The first column will 
-- be Category, that will contain the top 10 revenue categories; the second one 
-- will be Num_order, with the total amount of orders of each category; and the 
-- last one will be Revenue, with the total revenue of each catgory.
-- HINT: All orders should have a delivered status and the Category and actual 
-- delivery date should be not null.

SELECT
    pctn.product_category_name_english AS Category,
    COUNT(DISTINCT o.order_id) AS Num_order,
    SUM(pay.payment_value) AS Revenue
FROM
    olist_orders o
JOIN
    olist_order_items oi ON o.order_id = oi.order_id
JOIN
    olist_products p ON oi.product_id = p.product_id
JOIN
    product_category_name_translation pctn ON p.product_category_name = pctn.product_category_name
JOIN
    olist_order_payments pay ON o.order_id = pay.order_id
WHERE
    o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND pctn.product_category_name_english IS NOT NULL
GROUP BY
    pctn.product_category_name_english
ORDER BY
    Revenue DESC
LIMIT 10;
