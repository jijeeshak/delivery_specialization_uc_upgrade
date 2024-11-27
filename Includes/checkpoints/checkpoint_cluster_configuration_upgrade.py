# Databricks notebook source
# DBTITLE 1,def get_username()
def get_username():
  # Execute a SQL query to get the current user's email
  user_email = spark.sql('select current_user() as user').collect()[0]['user']
  # Extract the user ID from the email by splitting at the "@" character
  user_id = user_email.split("@")[0]
  return user_id

# Get the user ID by calling the get_username function
user_id = get_username()

# COMMAND ----------

# DBTITLE 1,test for existence of created_job_id
try:
  # Attempt to read the job_id from the specified table in the user's sandbox
  created_job_id = spark.read.table(f"{user_id}_sandbox2.workflow_upgrade.job_details").select('job_id').collect()[0][0]
except:
  # If an error occurs (e.g., table does not exist or job_id is not found), print an informative message
  print(f"The workflow has not been created. Please review the setup steps.")

# COMMAND ----------

# DBTITLE 1,workspace setup
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Retrieve the Databricks workspace URL from the Spark configuration
host_url = spark.conf.get("spark.databricks.workspaceUrl")

# Create a configuration object for the Databricks SDK
config = Config(
    # Set the host URL for the Databricks workspace
    host=f"https://{host_url}",
    # Retrieve the API token for authentication from the Databricks notebook context
    token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
)

# Initialize the WorkspaceClient with the specified configuration
w = WorkspaceClient(config=config)

# COMMAND ----------

# DBTITLE 1,def initial_workflow_upgrade_checkpoint()
from databricks.sdk.service.compute import DataSecurityMode

def initial_workflow_upgrade_checkpoint(created_job_cluster):
    # Retrieve the data security mode of the new cluster
    data_security_mode = created_job_cluster.new_cluster.data_security_mode
    # Retrieve the Spark version of the new cluster
    spark_version = created_job_cluster.new_cluster.spark_version
    # Retrieve the Spark configurations of the new cluster
    spark_configs = created_job_cluster.new_cluster.spark_conf

    try:
        # Check if the data security mode is set to SINGLE_USER
        if data_security_mode == DataSecurityMode.SINGLE_USER:
            print("Data security mode is set to SINGLE_USER.")
            return 

        # Check if the Spark version is set to 15.4 LTS
        if spark_version != "15.4.x-scala2.12":
            print("Spark version should be set to 15.4 LTS.")
            return 

        # Check if the Spark configurations are set and contain the initial catalog name
        if not spark_configs or 'spark.databricks.sql.initial.catalog.name' not in spark_configs:
            print("Default catalog name has not been set in the cluster's Spark configurations.")
            return 

        # Check if the initial catalog name is correctly set to the user's sandbox
        if spark_configs['spark.databricks.sql.initial.catalog.name'] != f'{user_id}_sandbox2':
            print("Incorrect default catalog name has been set on the cluster.")
            return 

        # If all checks pass, print a success message
        print("Passed cluster configuration checks.")
        return 

    # Handle any exceptions that occur during the configuration checks
    except Exception as e:
        print(f"An error occurred during the cluster configuration check: {e}")
        return

# COMMAND ----------

# DBTITLE 1,initial_workflow_upgrade_checkpoint(created_job_cluster)
try:
    # Retrieve the first job cluster settings using the job ID
    created_job_cluster = w.jobs.get(job_id=created_job_id).settings.job_clusters[0]
    # Perform the initial workflow upgrade checkpoint on the retrieved job cluster
    initial_workflow_upgrade_checkpoint(created_job_cluster)
except:
    # Print an error message if the job cluster cannot be retrieved
    print(f"The job cluster cannot be retrieved. Could not perform configuration checks. Please review the setup steps.")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>