SELECT 
    t.customer_product_id,
    p.product_name,
    p.sku,
    p.category_name,
    SUM(t.order_quantity) AS total_units_sold,
    SUM(t.line_total) AS total_revenue
FROM transaction_line_item t
JOIN product_data p 
    ON t.customer_product_id = p.customer_product_id
GROUP BY 
    t.customer_product_id, 
    p.product_name, 
    p.sku, 
    p.category_name
ORDER BY total_revenue DESC