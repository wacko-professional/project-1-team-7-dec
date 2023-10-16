# Databricks notebook source
bronze_categories_table_name = f"bronze_categories"

silver_categories_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/categories"
silver_categories_table_name = f"silver_categories"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_categories_df = spark.sql(f"""
    SELECT
      category_id,
      category_name,
      description
    FROM 
      {bronze_categories_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_categories_df.write.option("path", silver_categories_path).format("delta").mode("overwrite").saveAsTable(silver_categories_table_name)