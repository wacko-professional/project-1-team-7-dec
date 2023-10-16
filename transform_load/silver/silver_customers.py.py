# Databricks notebook source
bronze_customers_table_name = f"bronze_customers"

silver_customers_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/customers"
silver_customers_table_name = f"silver_customers"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_customers_df = spark.sql(f"""
    SELECT
      customer_id,
      company_name, 
      contact_name,
      contact_title,
      address,
      city,
      region,
      postal_code,
      country,
      phone,
      fax
    FROM 
      {bronze_customers_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_customers_df.write.option("path", silver_customers_path).format("delta").mode("overwrite").saveAsTable(silver_customers_table_name)