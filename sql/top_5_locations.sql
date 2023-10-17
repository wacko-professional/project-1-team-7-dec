-- top 5 locations

SELECT
    ship_country as location,
    COUNT(order_id) AS total_orders
FROM
    `hive_metastore`.`group_7`.`gold_orders_fact` 
GROUP BY
    ship_country
ORDER BY
    total_orders DESC
LIMIT 5;
