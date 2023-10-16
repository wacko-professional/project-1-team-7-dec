# Databricks notebook source
spark.sql("USE group_7")

# COMMAND ----------

silver_employees_table_name = "silver_employees"
silver_employee_territories_table_name = "silver_employee_territories"
silver_territories_table_name = "silver_territories"
silver_region_table_name = "silver_region"

gold_employees_dim_table_path = "dbfs:/mnt/dbacademy-users/group_7/gold/employees_dim"
gold_employees_dim_table_name = "gold_employees_dim"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_employees_dim_table_name}
USING DELTA
LOCATION "{gold_employees_dim_table_path}"
AS
SELECT 
    e.employee_id,
    e.last_name,
    e.first_name,
    e.title,
    e.title_of_courtesy,
    e.region,
    t.territory_description,
    r.region_description
FROM 
    {silver_employees_table_name} e
LEFT JOIN 
    {silver_employee_territories_table_name} et
ON 
    e.employee_id = et.employee_id
LEFT JOIN 
    {silver_territories_table_name} t
ON 
    et.territory_id = t.territory_id
LEFT JOIN 
    {silver_region_table_name} r
ON 
    t.region_id = r.region_id
""")