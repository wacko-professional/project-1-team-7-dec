# Databricks notebook source
bronze_products_table_name = f"bronze_products"

silver_products_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/products"
silver_products_table_name = f"silver_products"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_products_df = spark.sql(f"""
    SELECT
      product_id,
      product_name, 
      supplier_id,
      category_id,
      quantity_per_unit,
      unit_price,
      units_in_stock,
      units_on_order,
      reorder_level,
      discontinued
    FROM 
      {bronze_products_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_products_df.write.option("path", silver_products_path).format("delta").mode("overwrite").saveAsTable(silver_products_table_name)