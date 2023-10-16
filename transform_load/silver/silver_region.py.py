# Databricks notebook source
bronze_region_table_name = f"bronze_region"

silver_region_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/region"
silver_region_table_name = f"silver_region"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_region_df = spark.sql(f"""
    SELECT
      region_id,
      region_description
    FROM 
      {bronze_region_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_region_df.write.option("path", silver_region_path).format("delta").mode("overwrite").saveAsTable(silver_region_table_name)