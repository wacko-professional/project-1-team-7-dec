# Databricks notebook source
#%sql
#SHOW DATABASES;

# COMMAND ----------

spark.sql("USE group_7")

# COMMAND ----------

# MAGIC %pip install great_expectations

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# schemas
# 1. region
region_schema = StructType([
    StructField("region_id", IntegerType(), True),
    StructField("region_description", StringType(), True)
])

# 2. employees
employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("title_of_courtesy", StringType(), True),
    StructField("birth_date", StringType(), True),  # or DateType() based on date format
    StructField("hire_date", StringType(), True),  # or DateType() based on date format
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("home_phone", StringType(), True),
    StructField("extension", StringType(), True),
    StructField("photo", StringType(), True),
    StructField("notes", StringType(), True)
])

# 3. customer_customer_demo
customer_customer_demo_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_type_id", StringType(), True)
])

# 4. customer_demographics
customer_demographics_schema = StructType([
    StructField("customer_type_id", StringType(), True),
    StructField("customer_desc", StringType(), True)
])

# 5. orders
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("required_date", StringType(), True),
    StructField("shipped_date", StringType(), True),
    StructField("ship_via", IntegerType(), True),
    StructField("freight", DoubleType(), True),
    StructField("ship_name", StringType(), True),
    StructField("ship_address", StringType(), True),
    StructField("ship_city", StringType(), True),
    StructField("ship_region", StringType(), True),
    StructField("ship_postal_code", StringType(), True),
    StructField("ship_country", StringType(), True)
])

# 6. customers
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("contact_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("fax", StringType(), True)
])

# 7. categories
categories_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("category_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("picture", StringType(), True)  # BinaryType() might be more suitable if the field contains binary data
])

# 8. suppliers
suppliers_schema = StructType([
    StructField("supplier_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("contact_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("homepage", StringType(), True)
])

# 9. employee_territories
employee_territories_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("territory_id", StringType(), True)
])

# 10. order_details
order_details_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount", DoubleType(), True)
])

# 11. products
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("quantity_per_unit", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("units_in_stock", IntegerType(), True),
    StructField("units_on_order", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("discontinued", IntegerType(), True)
])

# 12. us_states
us_states_schema = StructType([
    StructField("state_id", IntegerType(), True),
    StructField("state_name", StringType(), True),
    StructField("state_abbr", StringType(), True),
    StructField("state_region", StringType(), True)
])

# 13. shippers
shippers_schema = StructType([
    StructField("shipper_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("phone", StringType(), True)
])

#14. territories
territories_schema = StructType([
    StructField("territory_id", StringType(), True),
    StructField("territory_description", StringType(), True),
    StructField("region_id", IntegerType(), True)
])

# COMMAND ----------

def normalize(df, schema):
    return (
        df.select(
            "_airbyte_ab_id",
            from_json(col("_airbyte_data"), schema).alias("data"),
            "_airbyte_emitted_at"
        )
        .select(
            "_airbyte_ab_id",
            "data.*",
            "_airbyte_emitted_at"
        )
    )

# COMMAND ----------

bronze_territories_df = normalize(spark.sql("SELECT * FROM group_7.`_airbyte_raw_territories`"), territories_schema)
bronze_region_df = normalize(spark.sql("SELECT * FROM group_7.`_airbyte_raw_region`"), region_schema)
bronze_employees_df = normalize(spark.sql("SELECT * FROM group_7.`_airbyte_raw_employees`"), employees_schema)
bronze_customer_customer_demo_df = normalize(spark.sql("SELECT * FROM group_7.`_airbyte_raw_customer_customer_demo`"), customer_customer_demo_schema)
bronze_orders_df = normalize(spark.sql("SELECT * FROM group_7.`_airbyte_raw_orders`"), orders_schema)
bronze_customers_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_customers"), customers_schema)
bronze_categories_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_categories"), categories_schema)
bronze_suppliers_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_suppliers"), suppliers_schema)
bronze_customer_demographics_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_customer_demographics"), customer_demographics_schema)
bronze_employee_territories_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_employee_territories"), employee_territories_schema)
bronze_order_details_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_order_details"), order_details_schema)
bronze_products_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_products"), products_schema)
bronze_us_states_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_us_states"), us_states_schema)
bronze_shippers_df = normalize(spark.sql("SELECT * FROM group_7._airbyte_raw_shippers"), shippers_schema)

# COMMAND ----------

ge_bronze_orders_df = SparkDFDataset(bronze_orders_df)
orders_expectation_1 = ge_bronze_orders_df.expect_column_values_to_not_be_null("order_id")
if not orders_expectation_1["success"]: 
    raise Exception(orders_expectation_1)

# COMMAND ----------

ge_bronze_order_details_df = SparkDFDataset(bronze_order_details_df)

order_details_expectation_1 = ge_bronze_order_details_df.expect_column_values_to_not_be_null("order_id")
if not order_details_expectation_1["success"]: 
    raise Exception(order_details_expectation_1)

order_details_expectation_2 = ge_bronze_order_details_df.expect_column_values_to_not_be_null("product_id")
if not order_details_expectation_2["success"]: 
    raise Exception(order_details_expectation_2)

# COMMAND ----------

ge_bronze_shippers_df = SparkDFDataset(bronze_shippers_df)

shippers_expectation_1 = ge_bronze_shippers_df.expect_column_values_to_not_be_null("shipper_id")
if not shippers_expectation_1["success"]: 
    raise Exception(shippers_expectation_1)

# COMMAND ----------

bronze_order_details_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/order_details"
bronze_order_details_table_name = f"bronze_order_details"

bronze_order_details_df.write.option("path", bronze_order_details_path).format("delta").mode("overwrite").saveAsTable(bronze_order_details_table_name)

# COMMAND ----------

bronze_orders_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/orders"
bronze_orders_table_name = f"bronze_orders"

bronze_orders_df.write.option("path", bronze_orders_path).format("delta").mode("overwrite").saveAsTable(bronze_orders_table_name)

# COMMAND ----------

bronze_customers_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/customers"
bronze_customers_table_name = f"bronze_customers"

bronze_customers_df.write.option("path", bronze_customers_path).format("delta").mode("overwrite").saveAsTable(bronze_customers_table_name)

# COMMAND ----------

bronze_employees_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/employees"
bronze_employees_table_name = f"bronze_employees"

bronze_employees_df.write.option("path", bronze_employees_path).format("delta").mode("overwrite").saveAsTable(bronze_employees_table_name)

# COMMAND ----------

bronze_products_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/products"
bronze_products_table_name = f"bronze_products"

bronze_products_df.write.option("path", bronze_products_path).format("delta").mode("overwrite").saveAsTable(bronze_products_table_name)

# COMMAND ----------

bronze_shippers_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/shippers"
bronze_shippers_table_name = f"bronze_shippers"

bronze_shippers_df.write.option("path", bronze_shippers_path).format("delta").mode("overwrite").saveAsTable(bronze_shippers_table_name)

# COMMAND ----------

bronze_categories_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/categories"
bronze_categories_table_name = f"bronze_categories"

bronze_categories_df.write.option("path", bronze_categories_path).format("delta").mode("overwrite").saveAsTable(bronze_categories_table_name)

# COMMAND ----------

bronze_suppliers_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/suppliers"
bronze_suppliers_table_name = f"bronze_suppliers"

bronze_suppliers_df.write.option("path", bronze_suppliers_path).format("delta").mode("overwrite").saveAsTable(bronze_suppliers_table_name)

# COMMAND ----------

bronze_customer_customer_demo_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/customer_customer_demo"
bronze_customer_customer_demo_table_name = f"bronze_customer_customer_demo"

bronze_customer_customer_demo_df.write.option("path", bronze_customer_customer_demo_path).format("delta").mode("overwrite").saveAsTable(bronze_customer_customer_demo_table_name)

# COMMAND ----------

bronze_customer_demographics_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/customer_demographics"
bronze_customer_demographics_table_name = f"bronze_customer_demographics"

bronze_customer_demographics_df.write.option("path", bronze_customer_demographics_path).format("delta").mode("overwrite").saveAsTable(bronze_customer_demographics_table_name)

# COMMAND ----------

bronze_employee_territories_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/employee_territories"
bronze_employee_territories_table_name = f"bronze_employee_territories"

bronze_employee_territories_df.write.option("path", bronze_employee_territories_path).format("delta").mode("overwrite").saveAsTable(bronze_employee_territories_table_name)

# COMMAND ----------

bronze_employee_territories_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/employee_territories"
bronze_employee_territories_table_name = f"bronze_employee_territories"

bronze_employee_territories_df.write.option("path", bronze_employee_territories_path).format("delta").mode("overwrite").saveAsTable(bronze_employee_territories_table_name)

# COMMAND ----------

bronze_territories_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/territories"
bronze_territories_table_name = f"bronze_territories"

bronze_territories_df.write.option("path", bronze_territories_path).format("delta").mode("overwrite").saveAsTable(bronze_territories_table_name)

# COMMAND ----------

bronze_region_path = f"dbfs:/mnt/dbacademy-users/group_7/bronze/region"
bronze_region_table_name = f"bronze_region"

bronze_region_df.write.option("path", bronze_region_path).format("delta").mode("overwrite").saveAsTable(bronze_region_table_name)

# COMMAND ----------

#spark.catalog.listDatabases()