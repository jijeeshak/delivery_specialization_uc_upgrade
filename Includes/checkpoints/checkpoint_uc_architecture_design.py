# Databricks notebook source
# DBTITLE 1,def get_username()
def get_username():
    # Execute a SQL query to get the current user's email
    user_email = spark.sql('select current_user() as user').collect()[0]['user']
    
    # Extract the user ID from the email by splitting at the '@' character
    user_id = user_email.split("@")[0]
    
    # Return the extracted user ID
    return user_id

# Call the function to get the user ID and store it in the variable 'user_id'
user_id = get_username()

# COMMAND ----------

# DBTITLE 1,def test_catalog_exists()
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Retrieve the Databricks workspace URL from Spark configuration
host_url = spark.conf.get("spark.databricks.workspaceUrl")

# Create a configuration object for the WorkspaceClient using the workspace URL and API token
config = Config(
    host=f"https://{host_url}",
    token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
)

# Initialize the WorkspaceClient with the provided configuration
w = WorkspaceClient(config=config)

def test_catalog_exists(catalog_name, workspace_client, default_storage_location="", no_default_location=False):
    # Get the current user's email from Spark SQL
    user_email = spark.sql('SELECT current_user() AS user').collect()[0]['user']

    try:
        # Retrieve catalog information using the workspace client
        catalog_info = workspace_client.catalogs.get(catalog_name)

        # Check if the catalog exists
        if catalog_info.name is None:
            print(f"Catalog {catalog_name} does not exist.")
            return

        # Check if the current user is the owner of the catalog
        if catalog_info.owner != user_email:
            print(f"Current user is not the owner of the catalog {catalog_name}.")
            return

        # Check if the default storage location is specified correctly, if provided
        if default_storage_location:
            if catalog_info.storage_root != default_storage_location:
                print(f"Catalog {catalog_name} default storage location is not specified correctly.")
                return

        # Check if the default storage location should not be specified
        if no_default_location:
            if catalog_info.storage_root is not None:
                print(f"Catalog {catalog_name} default storage location should not be specified.")
                return
            
        # If all checks pass, print a success message
        print(f"Passed all checks for catalog '{catalog_name}'.")

    except Exception as e:
        # Handle exceptions and print an error message if the catalog does not exist
        print(f"Catalog '{catalog_name}' does not exist.")

# COMMAND ----------

# DBTITLE 1,def test_database_exists()
def test_database_exists(database_name, catalog_name, workspace_client, default_storage_location="", no_default_location=False):
    # Get the current user's email from Spark SQL
    user_email = spark.sql('SELECT current_user() AS user').collect()[0]['user']

    try:
        # Retrieve schema information using the workspace client
        db_info = workspace_client.schemas.get(f"{catalog_name}.{database_name}")

        # Check if the schema exists by comparing the full name
        if db_info.full_name != f"{catalog_name}.{database_name}":
            print(f"Schema {database_name} does not exist.")
            return

        # Check if the current user is the owner of the schema
        if db_info.owner != user_email:
            print(f"Current user is not the owner of the schema {database_name}.")
            return

        # Check if the default storage location is specified correctly, if provided
        if default_storage_location:
            if db_info.storage_root != default_storage_location:
                print(f"Schema {database_name} default storage location is not specified correctly.")
                return

        # Check if the default storage location should not be specified
        if no_default_location:
            if db_info.storage_root is not None:
                print(f"Schema {database_name} default storage location should not be specified.")
                return
            
        # If all checks pass, print a success message
        print(f"Passed all checks for schema '{database_name}'.")

    except Exception as e:
        # Handle exceptions and print an error message if the schema does not exist
        print(f"Schema '{database_name}' does not exist.")

# COMMAND ----------

# DBTITLE 1,def test_volume_exists()
from databricks.sdk.service.catalog import VolumeType

def test_volume_exists(volume_name, database_name, catalog_name, workspace_client):
  # Get the current user's email from Spark SQL
  user_email = spark.sql('select current_user() as user').collect()[0]['user']

  try:
    # Retrieve volume information using the workspace client
    volume_info = workspace_client.volumes.read(f"{catalog_name}.{database_name}.{volume_name}")
    
    # Check if the current user is the owner of the volume
    if volume_info.owner != user_email:
      print(f"Current user is not the owner of the volume {volume_name}.")
      return
    
    # Check if the volume type is managed
    if volume_info.volume_type != VolumeType.MANAGED:
      print(f"The volume type should be managed.")
      return
    
    # If all checks pass, print a success message
    print(f"Passed all checks for volume '{volume_name}'.")

  except:
    # Handle exceptions and print an error message if the volume does not exist
    print(f"Volume '{volume_name}' does not exist.")

# COMMAND ----------

# DBTITLE 1,test_catalog_exists() - sandbox1
# Test if the catalog exists with the specified name and no default location
test_catalog_exists(f"{user_id}_sandbox1", w, no_default_location=True)

# COMMAND ----------

# DBTITLE 1,test_catalog_exists() - sandbox2
# Test if the catalog exists with the specified name and no default location
test_catalog_exists(f"{user_id}_sandbox2", w, no_default_location=True)

# COMMAND ----------

# DBTITLE 1,test_database_exists() - airbnb_bronze_db
# Test if the database "airbnb_bronze_db" exists within the specified catalog and no default location
test_database_exists("airbnb_bronze_db", f"{user_id}_sandbox1", w, no_default_location=True)

# COMMAND ----------

# DBTITLE 1,test_database_exists() - airbnb_silver_db
# Test if the database "airbnb_silver_db" exists within the specified catalog and no default location
test_database_exists("airbnb_silver_db", f"{user_id}_sandbox1", w, no_default_location=True)

# COMMAND ----------

# DBTITLE 1,test_database_exists() - airbnb_gold_db
# Test if the database "airbnb_gold_db" exists within the specified catalog and no default location
test_database_exists("airbnb_gold_db", f"{user_id}_sandbox1", w, no_default_location=True)

# COMMAND ----------

# DBTITLE 1,test_database_exists() - tpch_db
# Test if the database "tpch_db" exists within the specified catalog and no default location
test_database_exists("tpch_db", f"{user_id}_sandbox2", w, no_default_location=True)

# COMMAND ----------

# DBTITLE 1,test_database_exists() - tpch_db.managed_volume
# Test if the volume "managed_volume" exists within the "tpch_db" database in the specified catalog
test_volume_exists("managed_volume", "tpch_db", f"{user_id}_sandbox2", w)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>