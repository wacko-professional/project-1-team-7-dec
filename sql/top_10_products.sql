-- top_10_products

SELECT
    p.product_name,
    p.quantity_per_unit,
    SUM(of.quantity) AS total_quantity_purchased
FROM
    `hive_metastore`.`group_7`.`gold_orders_fact` of
JOIN
    `hive_metastore`.`group_7`.`gold_products_dim` p ON of.product_id = p.product_id
GROUP BY
    p.product_name,
    p.quantity_per_unit
ORDER BY
    total_quantity_purchased DESC
LIMIT 10;