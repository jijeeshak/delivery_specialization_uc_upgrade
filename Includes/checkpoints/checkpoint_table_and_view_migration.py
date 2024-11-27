# Databricks notebook source
# DBTITLE 1,workspace setup
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Retrieve the Databricks workspace URL from Spark configuration
host_url = spark.conf.get("spark.databricks.workspaceUrl")

# Create a configuration object for the Databricks SDK
config = Config(
    host=f"https://{host_url}",  # Set the host URL for the Databricks workspace
    token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)  # Retrieve the API token for authentication
)

# Initialize the WorkspaceClient with the provided configuration
w = WorkspaceClient(config=config)

# COMMAND ----------

# DBTITLE 1,def get_username()
def get_username():
    # Execute a SQL query to get the current user's email
    user_email = spark.sql('select current_user() as user').collect()[0]['user']
    
    # Extract the user ID from the email by splitting at the '@' character
    user_id = user_email.split("@")[0]
    
    # Return the user ID
    return user_id

# Call the get_username function to retrieve the user ID and assign it to the variable user_id
user_id = get_username()

# COMMAND ----------

# DBTITLE 1,def check_individual_table_has_been_migrated()
from databricks.sdk.service.catalog import TableType, DataSourceFormat

def check_individual_table_has_been_migrated(w, catalog_name, schema_name, table_name, expected_count, data_source_format, managed=True):
    # Get the current user's email
    user_email = spark.sql('SELECT current_user() AS user').collect()[0]['user']
    
    try:
        # Retrieve table information from the Databricks workspace
        table_info = w.tables.get(full_name=f'{catalog_name}.{schema_name}.{table_name}')
        
        # Check if the table exists
        if table_info.full_name != f'{catalog_name}.{schema_name}.{table_name}':
            print(f"Table '{catalog_name}.{schema_name}.{table_name}' does not exist.")
            return
        
        # Check if the current user is the owner of the table
        if table_info.owner != user_email:
            print(f"Current user is not the owner of the table '{table_name}'.")
            return
        
        # Check if the table is managed or external based on the 'managed' flag
        if managed:
            if table_info.table_type != TableType.MANAGED:
                print(f"Table '{catalog_name}.{schema_name}.{table_name}' should be MANAGED but is '{table_info.table_type}'.")
                return
        else:
            if table_info.table_type != TableType.EXTERNAL:
                print(f"Table '{catalog_name}.{schema_name}.{table_name}' should be EXTERNAL but is '{table_info.table_type}'.")
                return
        
        # Check if the data source format matches the expected format
        if table_info.data_source_format != data_source_format:
            print(f"Data source format for table '{table_name}' should be '{data_source_format}' but is '{table_info.data_source_format}'.")
            return
        
        # Count the number of records in the table and compare with the expected count
        actual_count = spark.table(f'{catalog_name}.{schema_name}.{table_name}').count()
        
        if actual_count != expected_count:
            print(f"Incorrect number of records in table '{table_name}'. Expected '{expected_count}', found '{actual_count}'.")
            return
        
        # If all checks pass, print a success message
        print(f"Passed checks for '{catalog_name}.{schema_name}.{table_name}'")

    except Exception as e:
        # Handle exceptions and print an error message if the table does not exist
        print(f"Table '{catalog_name}.{schema_name}.{table_name}' does not exist.")

# COMMAND ----------

# DBTITLE 1,check_individual_table_has_been_migrated() - tpch_db
# Check if the 'region' table in the 'tpch_db' schema of the user's sandbox catalog has been migrated correctly
# Expected record count: 5, Data source format: DELTA, Managed table: True
check_individual_table_has_been_migrated(w, f"{user_id}_sandbox2", "tpch_db", "region", 5, DataSourceFormat.DELTA, managed=True)

# Check if the 'lineitem' table in the 'tpch_db' schema of the user's sandbox catalog has been migrated correctly
# Expected record count: 29,999,795, Data source format: DELTA, Managed table: True
check_individual_table_has_been_migrated(w, f"{user_id}_sandbox2", "tpch_db", "lineitem", 29999795, DataSourceFormat.DELTA, managed=True)

# Check if the 'orders' table in the 'tpch_db' schema of the user's sandbox catalog has been migrated correctly
# Expected record count: 7,500,000, Data source format: DELTA, Managed table: True
check_individual_table_has_been_migrated(w, f"{user_id}_sandbox2", "tpch_db", "orders", 7500000, DataSourceFormat.DELTA, managed=True)

# COMMAND ----------

# DBTITLE 1,def check_individual_view_has_been_migrated()

from databricks.sdk.service.catalog import TableType

def check_individual_view_has_been_migrated(w, catalog_name, schema_name, view_name, expected_count):
    # Get the current user's email
    user_email = spark.sql('SELECT current_user() AS user').collect()[0]['user']
    
    try:
        # Retrieve view information from the Databricks workspace
        view_info = w.tables.get(full_name=f'{catalog_name}.{schema_name}.{view_name}')
        
        # Check if the view exists
        if view_info.full_name != f'{catalog_name}.{schema_name}.{view_name}':
            print(f"View '{catalog_name}.{schema_name}.{view_name}' does not exist.")
            return
        
        # Check if the current user is the owner of the view
        if view_info.owner != user_email:
            print(f"Current user is not the owner of the view '{view_name}'.")
            return
        
        # Check if the object is a view
        if view_info.table_type != TableType.VIEW:
            print(f"'{catalog_name}.{schema_name}.{view_name}' should be a VIEW but is '{view_info.table_type}'.")
            return
        
        # Count the number of records in the view and compare with the expected count
        actual_count = spark.table(f'{catalog_name}.{schema_name}.{view_name}').count()
        
        if actual_count != expected_count:
            print(f"Incorrect number of records in view '{view_name}'. Expected '{expected_count}', found '{actual_count}'.")
            return
        
        # If all checks pass, print a success message
        print(f"Passed checks for '{catalog_name}.{schema_name}.{view_name}'")
        
    except Exception as e:
        # Handle exceptions and print an error message if the view does not exist
        print(f"View '{catalog_name}.{schema_name}.{view_name}' does not exist.")

# COMMAND ----------

# DBTITLE 1,check_individual_view_has_been_migrated() - airbnb_gold_db.vw_property
# Check if the 'vw_property' view in the 'airbnb_gold_db' schema of the user's sandbox catalog has been migrated correctly
# Expected record count: 18,167
check_individual_view_has_been_migrated(w, f"{user_id}_sandbox1", "airbnb_gold_db", "vw_property", 18167)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>