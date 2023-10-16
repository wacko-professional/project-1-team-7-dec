# Databricks notebook source
spark.sql("USE group_7")

# COMMAND ----------

silver_orders_table_name = "silver_orders"
silver_order_details_table_name = "silver_order_details"

gold_orders_fact_table_path = "dbfs:/mnt/dbacademy-users/group_7/gold/orders_fact"
gold_orders_fact_table_name = "gold_orders_fact"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_orders_fact_table_name}
USING DELTA
LOCATION "{gold_orders_fact_table_path}"
AS
SELECT 
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.ship_via,
    o.freight,
    o.ship_name,
    o.ship_address,
    o.ship_city,
    o.ship_region,
    o.ship_postal_code,
    o.ship_country,
    d.product_id,
    d.quantity,
    d.unit_price,
    d.discount,
    d.quantity * d.unit_price * (1 - d.discount) AS order_item_total
FROM 
    {silver_orders_table_name} o
JOIN 
    {silver_order_details_table_name} d
ON 
    o.order_id = d.order_id
""")