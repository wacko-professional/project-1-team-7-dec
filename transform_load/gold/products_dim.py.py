# Databricks notebook source
spark.sql("USE group_7")

# COMMAND ----------

silver_products_table_name = "silver_products"
silver_categories_table_name = "silver_categories"
silver_suppliers_table_name = "silver_suppliers"

gold_products_dim_table_path = "dbfs:/mnt/dbacademy-users/group_7/gold/products_dim"
gold_products_dim_table_name = "gold_products_dim"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_products_dim_table_name}
USING DELTA
LOCATION "{gold_products_dim_table_path}"
AS
SELECT 
    p.product_id,
    p.product_name,
    p.quantity_per_unit,
    p.unit_price,
    p.units_in_stock,
    p.units_on_order,
    p.reorder_level,
    p.discontinued,
    c.category_name,
    c.description AS category_description,
    s.company_name AS supplier_company_name,
    s.address AS supplier_address,
    s.city AS supplier_city,
    s.region AS supplier_region,
    s.postal_code AS supplier_postal_code,
    s.country AS supplier_country
FROM 
    {silver_products_table_name} p
JOIN 
    {silver_categories_table_name} c
ON 
    p.category_id = c.category_id
JOIN 
    {silver_suppliers_table_name} s
ON 
    p.supplier_id = s.supplier_id
""")