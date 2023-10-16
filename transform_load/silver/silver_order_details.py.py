# Databricks notebook source
bronze_order_details_table_name = f"bronze_order_details"

silver_order_details_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/order_details"
silver_order_details_table_name = f"silver_order_details"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_order_details_df = spark.sql(f"""
    SELECT
      order_id,
      product_id, 
      unit_price,
      quantity,
      discount
    FROM 
      {bronze_order_details_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_order_details_df.write.option("path", silver_order_details_path).format("delta").mode("overwrite").saveAsTable(silver_order_details_table_name)