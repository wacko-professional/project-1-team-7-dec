# Databricks notebook source
from pyspark.sql.functions import to_date

# COMMAND ----------

bronze_orders_table_name = f"bronze_orders"

silver_orders_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/orders"
silver_orders_table_name = f"silver_orders"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_orders_df = spark.sql(f"""
    SELECT
      order_id,
      customer_id, 
      employee_id,
      to_date(order_date, 'yyyy-MM-dd') as order_date,
      to_date(required_date, 'yyyy-MM-dd') as required_date,
      to_date(shipped_date, 'yyyy-MM-dd') as shipped_date,
      ship_via,
      freight,
      ship_name,
      ship_address,
      ship_city,
      ship_region,
      ship_postal_code,
      ship_country
    FROM 
      {bronze_orders_table_name}
""").dropDuplicates()

# COMMAND ----------

display(silver_orders_df)

# COMMAND ----------

silver_orders_df.write.option("path", silver_orders_path).format("delta").mode("overwrite").saveAsTable(silver_orders_table_name)