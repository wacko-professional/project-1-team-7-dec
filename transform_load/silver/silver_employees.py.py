# Databricks notebook source
bronze_employees_table_name = f"bronze_employees"

silver_employees_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/employees"
silver_employees_table_name = f"silver_employees"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_employees_df = spark.sql(f"""
    SELECT
      employee_id,
      last_name, 
      first_name,
      title,
      title_of_courtesy,
      region
    FROM 
      {bronze_employees_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_employees_df.write.option("path", silver_employees_path).format("delta").mode("overwrite").saveAsTable(silver_employees_table_name)