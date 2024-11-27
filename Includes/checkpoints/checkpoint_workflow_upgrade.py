# Databricks notebook source
# DBTITLE 1,def get_username()
def get_username():
    # Execute a SQL query to get the current user's email
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

# Retrieve the Databricks workspace URL from Spark configuration
host_url = spark.conf.get("spark.databricks.workspaceUrl")

# Create a configuration object for the Databricks SDK
config = Config(
    host=f"https://{host_url}",  # Set the host URL for the Databricks workspace
    token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)  # Retrieve the API token for authentication
)

# Initialize the WorkspaceClient with the specified configuration
w = WorkspaceClient(config=config)

# COMMAND ----------

# DBTITLE 1,def final_workflow_upgrade_checkpoint()
from databricks.sdk.service.jobs import RunResultState

def final_workflow_upgrade_checkpoint(w, created_job_id):
    # List all runs for the specified job ID
    job_run_list = w.jobs.list_runs(job_id=created_job_id)

    # Extract the result state of each job run
    run_states = [job.state.result_state for job in job_run_list]

    # Check if there are no runs for the job
    if not run_states:
        print("Workflow has not been executed yet.")
        return 

    # Check if the most recent run was not successful
    if run_states[0] != RunResultState.SUCCESS:
        print("Workflow did not run successfully.")
        return 
    
    # If the most recent run was successful, print a success message
    print("Passed workflow upgrade checks.")

# COMMAND ----------

# DBTITLE 1,final_workflow_upgrade_checkpoint()
try:
    # Retrieve the job ID from the specified table in the user's sandbox
    created_job_id = spark.read.table(f"{user_id}_sandbox2.workflow_upgrade.job_details").select('job_id').collect()[0][0]
    
    # Perform the workflow upgrade checkpoint using the retrieved job ID
    final_workflow_upgrade_checkpoint(w, created_job_id)
except:
    # Handle the case where the workflow has not been created
    print(f"The workflow has not been created. Could not perform workflow upgrade check. Please review the setup steps.")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>