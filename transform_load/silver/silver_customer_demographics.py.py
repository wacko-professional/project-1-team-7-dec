# Databricks notebook source
bronze_customer_demographics_table_name = f"bronze_customer_demographics"

silver_customer_demographics_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/customer_demographics"
silver_customer_demographics_table_name = f"silver_customer_demographics"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_customer_demographics_df = spark.sql(f"""
    SELECT
      customer_type_id,
      customer_desc
    FROM 
      {bronze_customer_demographics_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_customer_demographics_df.write.option("path", silver_customer_demographics_path).format("delta").mode("overwrite").saveAsTable(silver_customer_demographics_table_name)