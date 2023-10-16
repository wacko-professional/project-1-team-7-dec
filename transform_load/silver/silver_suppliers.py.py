# Databricks notebook source
bronze_suppliers_table_name = f"bronze_suppliers"

silver_suppliers_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/suppliers"
silver_suppliers_table_name = f"silver_suppliers"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_suppliers_df = spark.sql(f"""
    SELECT
      supplier_id,
      company_name,
      address,
      city,
      region,
      postal_code,
      country
    FROM 
      {bronze_suppliers_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_suppliers_df.write.option("path", silver_suppliers_path).format("delta").mode("overwrite").saveAsTable(silver_suppliers_table_name)