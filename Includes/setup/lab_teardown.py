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

# DBTITLE 1,workspace setup
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Retrieve the Databricks workspace URL from the Spark configuration
host_url = spark.conf.get("spark.databricks.workspaceUrl")

# Create a configuration object for the Databricks SDK
config = Config(
    host=f"https://{host_url}",  # Set the host URL for the Databricks workspace
    token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)  # Retrieve the API token for authentication
)

# Initialize the WorkspaceClient with the specified configuration
w = WorkspaceClient(config=config)

# COMMAND ----------

# DBTITLE 1,delete job_id_to_delete
try:
  # Read the job details table specific to the user and select the first job_id
  job_id_to_delete = spark.read.table(f"{user_id}_sandbox2.workflow_upgrade.job_details").select("job_id").collect()[0][0]

  # Delete the job with the specified job_id using the WorkspaceClient
  w.jobs.delete(job_id=job_id_to_delete)

except:
  print("No workflow found to delete. Proceeding with teardown processes.")

# COMMAND ----------

# DBTITLE 1,DROP hive_metastore airbnb and tpch databases
# Define the catalog name
catalog_name = "hive_metastore"

# Construct the database name for the Airbnb Bronze database and drop it if it exists
db_name = user_id + "_airbnb_bronze_db"
res = spark.sql(f"drop database if exists {catalog_name}.{db_name} CASCADE")

# Construct the database name for the Airbnb Silver database and drop it if it exists
db_name = user_id + "_airbnb_silver_db"
res = spark.sql(f"drop database if exists {catalog_name}.{db_name} CASCADE")

# Construct the database name for the Airbnb Gold database and drop it if it exists
db_name = user_id + "_airbnb_gold_db"
res = spark.sql(f"drop database if exists {catalog_name}.{db_name} CASCADE")

# Construct the database name for the TPCH database and drop it if it exists
db_name = user_id + "_tpch_db"
res = spark.sql(f"drop database if exists {catalog_name}.{db_name} CASCADE")

# COMMAND ----------

# DBTITLE 1,DROP unity catalog user catalogs - sandbox1 & sandbox2
# Drop the catalog if it exists for the first sandbox environment
res = spark.sql(f"drop catalog if exists {user_id}_sandbox1 CASCADE")

# Drop the catalog if it exists for the second sandbox environment
res = spark.sql(f"drop catalog if exists {user_id}_sandbox2 CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>