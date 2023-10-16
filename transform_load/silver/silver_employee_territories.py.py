# Databricks notebook source
bronze_employee_territories_table_name = f"bronze_employee_territories"

silver_employee_territories_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/employee_territories"
silver_employee_territories_table_name = f"silver_employee_territories"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_employee_territories_df = spark.sql(f"""
    SELECT
      employee_id,
      territory_id
    FROM 
      {bronze_employee_territories_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_employee_territories_df.write.option("path", silver_employee_territories_path).format("delta").mode("overwrite").saveAsTable(silver_employee_territories_table_name)