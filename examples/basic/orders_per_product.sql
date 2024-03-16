SELECT product_name, count() as item_count
FROM source_orders
GROUP BY product_name
