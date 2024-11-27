# Databricks notebook source
# DBTITLE 1,def get_username()
def get_username():
  # Execute a SQL query to get the current user's email
  user_email = spark.sql('select current_user() as user').collect()[0]['user']
  
  # Extract the user ID from the email by splitting at the "@" character
  user_id = user_email.split("@")[0]
  
  # Return the user ID
  return user_id

# COMMAND ----------

# DBTITLE 1,get user_id
# Get the user ID by calling the get_username function
user_id = get_username()

# COMMAND ----------

# DBTITLE 1,create hive_metastore tables and view
catalog_name="hive_metastore"

# Create the Bronze database for Airbnb data if it does not exist
db_name = user_id + "_airbnb_bronze_db"
res = spark.sql(f"create database if not exists {catalog_name}.{db_name}")

# Create the Silver database for Airbnb data if it does not exist
db_name = user_id + "_airbnb_silver_db"
res = spark.sql(f"create database if not exists {catalog_name}.{db_name}")

# Create the Gold database for Airbnb data if it does not exist
db_name = user_id + "_airbnb_gold_db"
res = spark.sql(f"create database if not exists {catalog_name}.{db_name}")

# Create the TPCH database if it does not exist
db_name = user_id + "_tpch_db"
res = spark.sql(f"create database if not exists {catalog_name}.{db_name}")

# COMMAND ----------

# DBTITLE 1,def create_bronze_airbnb_tables()
db_name = user_id + "_airbnb_bronze_db"

def create_bronze_airbnb_tables(db_name):
    # List of table names to be created in the Bronze database
    tables = ["listings", "neighbourhoods", "reviews"]

    for table in tables:
        # Define the table name with a "_bronze" suffix
        table_name = f"{table}_bronze"

        # SQL query to drop the table if it already exists
        drop_table_query = f"""DROP TABLE IF EXISTS hive_metastore.`{db_name}`.`{table_name}`;"""

        # Execute the drop table query
        spark.sql(drop_table_query)

        # Define the path to the source data volume
        volume_path = f"/Volumes/uc_upgrade_lab/airbnb_source_data/csv_vol/{table}" 

        # Read the CSV data into a DataFrame with specified options
        df = spark.read.format("csv").option("header", "true").option("multiLine", "true").option("inferSchema", "true").option("escape", '"').load(volume_path)

        # Write the DataFrame to the Bronze database as a table
        df.write.format("csv").mode("overwrite").saveAsTable(f"hive_metastore.{db_name}.{table_name}")

# Call the function to create the Bronze Airbnb tables
create_bronze_airbnb_tables(db_name)

# COMMAND ----------

# DBTITLE 1,def ETL functions (6)
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, trim, lower, initcap, col
import re

def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Adds an audit column 'ingestion_date' with the current timestamp 
    to the DataFrame to track the ingestion time.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with the 'ingestion_date' column added.
    """
    try:
        # Add a new column 'ingestion_date' with the current timestamp
        return df.withColumn("ingestion_date", current_timestamp())
    except Exception as e:
        print(e)

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Cleans column names by converting them to lowercase and replacing 
    non-alphanumeric characters with underscores.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with cleaned column names.
    """
    try:
        # Clean each column name: convert to lowercase and replace non-alphanumeric characters with underscores
        cleaned_columns = [col(col_name).alias(re.sub(r'\W+', '_', col_name.lower())) for col_name in df.columns]
        return df.select(*cleaned_columns)
    except Exception as e:
        print(e)

def trim_string_data(df: DataFrame) -> DataFrame:
    """
    Trims leading and trailing spaces from all string columns in the DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with trimmed string data.
    """
    try:
        # Identify string columns
        string_columns = [col_name for col_name, dtype in df.dtypes if dtype == 'string']
        
        # Apply trim to the data in all string columns
        for col_name in string_columns:
            df = df.withColumn(col_name, trim(col(col_name)))
        
        return df
    except Exception as e:
        print(e)

def convert_data_to_lowercase(df: DataFrame, columns: list) -> DataFrame:
    """
    Converts the data in specified columns to lowercase.
    
    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to convert to lowercase.

    Returns:
        DataFrame: The DataFrame with specified columns in lowercase.
    """
    try:
        # Apply lowercase transformation to the data in specified columns
        for col_name in columns:
            df = df.withColumn(col_name, lower(col(col_name)))
        
        return df
    except Exception as e:
        print(e)

def convert_data_to_title_case(df: DataFrame, columns: list) -> DataFrame:
    """
    Converts the data in specified columns to title case 
    (capitalizing the first letter of each word).
    
    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to convert to title case.

    Returns:
        DataFrame: The DataFrame with specified columns in title case.
    """
    try:
        # Apply title case transformation to the data in specified columns
        for col_name in columns:
            df = df.withColumn(col_name, initcap(col(col_name)))
        
        return df
    except Exception as e:
        print(e)

def standardize_dataframe(df: DataFrame) -> DataFrame:
    """
    Standardizes the DataFrame by cleaning column names, dropping duplicates, 
    adding audit columns, and trimming string data.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The standardized DataFrame.
    """
    try:
        # Clean the column names
        df = clean_column_names(df)

        # Drop duplicates
        df = df.dropDuplicates()
        
        # Add audit columns
        df = add_audit_columns(df)

        # Trim string columns data
        df = trim_string_data(df)
            
        return df
    
    except Exception as e:
        print(e)

# COMMAND ----------

# DBTITLE 1,create bronze and silver db names
# Define the Bronze database name using the user_id
bronze_db_name = user_id + "_airbnb_bronze_db"

# Define the Silver database name using the user_id
silver_db_name = user_id + "_airbnb_silver_db"

# COMMAND ----------

# DBTITLE 1,def save_airbnb_listing_data()
def save_airbnb_listing_data() -> None:
    try:
        # Load the Bronze DataFrame from Hive Metastore
        bronze_df_listing = spark.table(f"hive_metastore.{bronze_db_name}.listings_bronze")

        # Clean and standardize the DataFrame
        df_silver = standardize_dataframe(bronze_df_listing)

        # Convert specific columns to lowercase
        df_silver = convert_data_to_lowercase(
            df_silver, ['listing_url', 'source', 'picture_url', 'host_url']
        )

        # Convert specific columns to title case
        df_silver = convert_data_to_title_case(
            df_silver, ['property_type', 'room_type', 'host_name', 'source']
        )

        # Save the DataFrame as a Delta table in the Silver layer
        (
            df_silver.write
            .format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .saveAsTable(f"hive_metastore.{silver_db_name}.listings_silver")
        )

    except Exception as e:
        # Print the exception if any error occurs
        print(e)

# Execute the function to save the Airbnb listing data
save_airbnb_listing_data()

# COMMAND ----------

# DBTITLE 1,def save_airbnb_reviews_data()
def save_airbnb_reviews_data() -> None:
    try:
        # Load the Bronze DataFrame from Hive Metastore
        bronze_df_reviews= spark.table(f"hive_metastore.{bronze_db_name}.reviews_bronze")

        # Clean and standardize the DataFrame
        df_silver = standardize_dataframe(bronze_df_reviews)

        # Convert specific columns to title case
        df_silver = convert_data_to_title_case(
            df_silver, ['reviewer_name']
        )

        # Save the DataFrame as a Delta table in the Silver layer
        (
            df_silver.write
            .format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .saveAsTable(f"hive_metastore.{silver_db_name}.reviews_silver")
        )
    except Exception as e:
        # Print the exception if any error occurs
        print(e)

# Execute the function to save the Airbnb reviews data
save_airbnb_reviews_data()

# COMMAND ----------

# DBTITLE 1,def save_airbnb_neighbourhood_data()
def save_airbnb_neighbourhood_data() -> None:
    try:
        # Load the Bronze DataFrame from Hive Metastore
        bronze_df_neighbourhood= spark.table(f"hive_metastore.{bronze_db_name}.neighbourhoods_bronze")

        # Clean and standardize the DataFrame
        df_silver = standardize_dataframe(bronze_df_neighbourhood)

        # Convert specific columns to title case
        df_silver = convert_data_to_title_case(
            df_silver, ['neighbourhood']
        )

        # Save the DataFrame as a Delta table in the Silver layer
        (
            df_silver.write
            .format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .saveAsTable(f"hive_metastore.{silver_db_name}.neighbourhoods_silver")
        )
    except Exception as e:
        # Print the exception if any error occurs
        print(e)

# Execute the function to save the Airbnb neighbourhood data
save_airbnb_neighbourhood_data()

# COMMAND ----------

# DBTITLE 1,create or replace airbnb_gold_db.vw_property view
# Set the current catalog to hive_metastore
res = spark.sql("USE CATALOG hive_metastore;")

# Create or replace a view in the user's Airbnb Gold database
res = spark.sql(
    f"""
            CREATE OR REPLACE VIEW {user_id}_airbnb_gold_db.vw_property AS
            SELECT 
            prop.id AS property_id,  -- Select property ID
            prop.name AS property_name,  -- Select property name
            prop.host_id,  -- Select host ID
            COUNT(DISTINCT rev.id) AS total_reviews,  -- Count distinct reviews for each property
            AVG(CAST(REPLACE(prop.review_scores_value, 'N/A', '0') AS DECIMAL(4, 2))) AS avg_rating,  -- Calculate average rating, replacing 'N/A' with 0
            AVG(prop.reviews_per_month) AS avg_reviews_per_month  -- Calculate average reviews per month
            FROM {user_id}_airbnb_silver_db.listings_silver prop  -- From listings silver table
            LEFT JOIN {user_id}_airbnb_silver_db.reviews_silver rev ON prop.id = rev.listing_id  -- Left join with reviews silver table on listing ID
            GROUP BY 
            prop.id,  -- Group by property ID
            prop.name,  -- Group by property name
            prop.host_id  -- Group by host ID
            """
)

# COMMAND ----------

# DBTITLE 1,read & write tpch_db tables
# List of table names to be processed
tables = ["orders", "lineitem", "region"]

# Loop through each table in the list
for table in tables:
  # Read the table from the samples.tpch database
  df = spark.read.table(f"samples.tpch.{table}")
  
  # Construct the target database name using the user_id
  db_name = user_id + "_tpch_db"
  
  # Write the DataFrame to the target database in the Hive Metastore, overwriting any existing data
  df.write.mode("overwrite").saveAsTable(f"hive_metastore.{db_name}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>