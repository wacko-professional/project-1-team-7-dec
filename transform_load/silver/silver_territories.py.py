# Databricks notebook source
bronze_territories_table_name = f"bronze_territories"

silver_territories_path = f"dbfs:/mnt/dbacademy-users/group_7/silver/territories"
silver_territories_table_name = f"silver_territories"

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

silver_territories_df = spark.sql(f"""
    SELECT
      territory_id,
      territory_description,
      region_id
    FROM 
      {bronze_territories_table_name}
""").dropDuplicates()

# COMMAND ----------

silver_territories_df.write.option("path", silver_territories_path).format("delta").mode("overwrite").saveAsTable(silver_territories_table_name)