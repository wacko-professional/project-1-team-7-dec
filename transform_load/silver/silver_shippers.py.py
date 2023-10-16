# Databricks notebook source
bronze_shippers_table_name = f"bronze_shippers"

silver_shippers_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/shippers"
silver_shippers_table_name = f"silver_shippers"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_shippers_df = spark.sql(f"""
    SELECT
      shipper_id,
      company_name
    FROM 
      {bronze_shippers_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_shippers_df.write.option("path", silver_shippers_path).format("delta").mode("overwrite").saveAsTable(silver_shippers_table_name)