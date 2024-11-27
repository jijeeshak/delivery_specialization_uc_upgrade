# Databricks notebook source
# DBTITLE 1,def get_username()
def get_username():
  # Retrieve the current user's email from the Spark SQL context
  user_email = spark.sql('select current_user() as user').collect()[0]['user']
  
  # Extract the user ID from the email by splitting at the '@' character
  user_id = user_email.split("@")[0]
  
  return user_id

# Get the user ID by calling the get_username function
user_id = get_username()

# COMMAND ----------

# DBTITLE 1,create sandbox1.airbnb_bronze_db tables
tables =  ["listings", "neighbourhoods", "reviews"]  # List of table names to be processed
for tbl in tables:

  table_name = f"{tbl}_bronze"  # Construct the table name for the bronze layer
  uc_catalog_name = f'{user_id}_sandbox1'  # Construct the user-specific catalog name
  hms_db_name = user_id + "_airbnb_bronze_db"  # Construct the Hive Metastore database name for the bronze layer

  res = spark.sql(f"""
            CREATE TABLE `{uc_catalog_name}`.airbnb_bronze_db.{table_name} AS
            SELECT * FROM hive_metastore.`{hms_db_name}`.{table_name};
            """)  # Create a new table in the user-specific catalog by selecting data from the Hive Metastore

# COMMAND ----------

# DBTITLE 1,create sandbox1.airbnb_silver_db tables
tables =  ["listings", "neighbourhoods", "reviews"]  # List of table names to be processed
for tbl in tables:

  table_name = f"{tbl}_silver"  # Construct the table name for the silver layer
  uc_catalog_name = f'{user_id}_sandbox1'  # Construct the user-specific catalog name
  hms_db_name = user_id + "_airbnb_silver_db"  # Construct the Hive Metastore database name for the silver layer

  res = spark.sql(f"""
            CREATE TABLE `{uc_catalog_name}`.airbnb_silver_db.`{table_name}` AS
            SELECT * FROM hive_metastore.`{hms_db_name}`.`{table_name}`;
            """)  # Create a new table in the user-specific catalog by selecting data from the Hive Metastore

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>