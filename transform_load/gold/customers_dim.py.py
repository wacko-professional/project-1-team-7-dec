# Databricks notebook source
spark.sql("USE group_7")

# COMMAND ----------

silver_customers_table_name = "silver_customers"
silver_customer_customer_demo_table_name = "silver_customer_customer_demo"
silver_customer_demographics_table_name = "silver_customer_demographics"

gold_customers_dim_table_path = "dbfs:/mnt/dbacademy-users/group_7/gold/customers_dim"
gold_customers_dim_table_name = "gold_customers_dim"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_customers_dim_table_name}
USING DELTA
LOCATION "{gold_customers_dim_table_path}"
AS
SELECT 
    c.customer_id,
    c.company_name,
    c.contact_name,
    c.contact_title,
    c.address,
    c.city,
    c.region,
    c.postal_code,
    c.country,
    c.phone,
    c.fax,
    cd.customer_desc
FROM 
    {silver_customers_table_name} c
JOIN 
    {silver_customer_customer_demo_table_name} ccd
ON 
    c.customer_id = ccd.customer_id
JOIN 
    {silver_customer_demographics_table_name} cd
ON 
    ccd.customer_type_id = cd.customer_type_id
""")