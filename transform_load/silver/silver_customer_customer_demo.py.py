# Databricks notebook source
bronze_customer_customer_demo_table_name = f"bronze_customer_customer_demo"

silver_customer_customer_demo_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/customer_customer_demo"
silver_customer_customer_demo_table_name = f"silver_customer_customer_demo"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_customer_customer_demo_df = spark.sql(f"""
    SELECT
      customer_id,
      customer_type_id
    FROM 
      {bronze_customer_customer_demo_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_customer_customer_demo_df.write.option("path", silver_customer_customer_demo_path).format("delta").mode("overwrite").saveAsTable(silver_customer_customer_demo_table_name)