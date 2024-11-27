# Databricks notebook source
# DBTITLE 1,upgrade databricks sdk
# MAGIC %%capture
# MAGIC # Install or upgrade the databricks-sdk package to ensure we have the latest version
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

# DBTITLE 1,restart python
# Restart the Python process for the current notebook session.
# This is often necessary after installing or upgrading packages to ensure
# that the new versions are properly loaded and used in the notebook.
dbutils.library.restartPython()

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

# DBTITLE 1,def get_username()
def get_username():
    # Execute a SQL query to get the current user's email
    user_email = spark.sql('select current_user() as user').collect()[0]['user']
    
    # Extract the user ID from the email by splitting at the "@" character
    user_id = user_email.split("@")[0]
    
    # Return the user ID and email as a tuple
    return user_id, user_email

# COMMAND ----------

# DBTITLE 1,retrieve user_id, user_email
# Retrieve the user ID and email by calling the get_username function
user_id, user_email = get_username()

# COMMAND ----------

# DBTITLE 1,def db_check_if_cluster_policy_exists()
def db_check_if_cluster_policy_exists(w, policy_name):
    # List all cluster policies using the Databricks workspace client
    all_policies = w.cluster_policies.list()

    # Iterate through each policy to check if the policy name matches the given policy_name
    for policy in all_policies:
        if policy.name == policy_name:
            # If a match is found, return the policy_id
            return policy.policy_id
        
    # If no match is found, return an empty string
    return ""
  
# Check if the cluster policy "hms_uc_upgrade_workflow_policy" exists and retrieve its policy_id
workflow_policy_id = db_check_if_cluster_policy_exists(w, "hms_uc_upgrade_workflow_policy")

# COMMAND ----------

# DBTITLE 1,configure and create job cluster
from databricks.sdk.service import jobs
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode

# Define the path to the notebook that will be used for the upgrade
notebook_path = f'/Workspace/Users/{user_email}/delivery_specialization_uc_upgrade/Includes/assets/notebook_to_upgrade_to_uc'

# Define the job cluster configuration
job_clusters = [
    jobs.JobCluster(
        job_cluster_key=f'{user_id}_job_cluster',  # Unique key for the job cluster
        new_cluster=ClusterSpec(
            node_type_id='m5d.large',  # Specify the node type
            spark_version="10.4.x-scala2.12",  # Specify the Spark version
            num_workers=1,  # Number of worker nodes
            data_security_mode=DataSecurityMode.NONE,  # Data security mode
            policy_id= workflow_policy_id  # Cluster policy ID
        ),
    ),
]

# Create a new job with the specified configuration
created_job = w.jobs.create(
    name=f'{user_id}_uc_workflow_upgrade',  # Name of the job
    job_clusters=job_clusters,  # Job cluster configuration
    tasks=[
        jobs.Task(
            description="Workflow upgrade task",  # Description of the task
            job_cluster_key=f'{user_id}_job_cluster',  # Key for the job cluster to use
            notebook_task=jobs.NotebookTask(notebook_path=notebook_path),  # Notebook task configuration
            task_key="upgrade_task",  # Unique key for the task
            timeout_seconds=0  # Timeout for the task (0 means no timeout)
        )
    ]
)

# Retrieve the job ID of the created job
created_job_id = created_job.job_id

# COMMAND ----------

# DBTITLE 1,create workflow_upgrade database
# Create a database if it does not already exist
# The database name is dynamically generated using the user_id
res = spark.sql(f"CREATE DATABASE IF NOT EXISTS {user_id}_sandbox2.workflow_upgrade")

# COMMAND ----------

# DBTITLE 1,write workflow_upgrade_job_details
# Create a DataFrame with the user_id and created_job_id
df = spark.createDataFrame(
    [
        (user_id, created_job_id),  # Tuple containing user_id and created_job_id
    ],
    ["user_id", "job_id"]  # Column names for the DataFrame
)

# Write the DataFrame to a table in the specified database
# Overwrite mode ensures that the table is replaced if it already exists
df.write.mode("overwrite").saveAsTable(f"{user_id}_sandbox2.workflow_upgrade.job_details")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>