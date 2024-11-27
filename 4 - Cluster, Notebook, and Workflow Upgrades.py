# Databricks notebook source
# MAGIC %md
# MAGIC <div style ="text-align: center; line-height: 0; padding-top: 9px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 4: Upgrading Clusters, Notebooks, and Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, you will focus on upgrading clusters, refactoring notebooks, and updating workflows to be compatible with Unity Catalog. This involves adjusting cluster configurations, modifying code to use higher-level APIs, and ensuring workflows are correctly pointing to Unity Catalog resources.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SETUP: IMMEDIATELY RUN THE HMS WORKFLOW SETUP CELL BELOW

# COMMAND ----------

# DBTITLE 1,RUN hms_workflow_setup
# MAGIC %run ./Includes/setup/hms_workflow_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Activities

# COMMAND ----------

# MAGIC %md
# MAGIC As part of the initial workspace setup, the following tasks have been performed:
# MAGIC
# MAGIC - **Cluster Creation**:
# MAGIC   - Provisioned various cluster types for your use:
# MAGIC     - **Single User** and **Shared Compute** clusters.
# MAGIC     - **SQL Warehouses**.
# MAGIC     - Included **init scripts** where necessary.
# MAGIC - **Notebook Development**:
# MAGIC   - Created a notebook that utilizes the **Resilient Distributed Dataset (RDD) API**.
# MAGIC - **Workflow Establishment**:
# MAGIC   - Configured a workflow that uses the **Hive Metastore (HMS)** with a two-level namespace and applies HMS cluster configurations.
# MAGIC
# MAGIC **Note:** Setting up clusters and workflows may require elevated privileges, such as those held by an **Account Admin** or **Cloud Admin**. These setup steps have been completed by users with the necessary permissions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Details for Each User

# COMMAND ----------

# MAGIC %md
# MAGIC The following configurations have been applied for each user:
# MAGIC
# MAGIC - **Cluster Configuration**:
# MAGIC   - **Cluster Mode**: Shared (no isolation).
# MAGIC   - **Photon Acceleration**: Disabled.
# MAGIC   - **Databricks Runtime Version**: 10.4 LTS.
# MAGIC   - **Number of Workers**: 1.
# MAGIC   - **Instance Profile**: Not assigned.
# MAGIC - **Job Creation**:
# MAGIC   - A job has been created utilizing the above cluster and includes the notebook you will be working on.
# MAGIC
# MAGIC **Note:** Upgrading clusters to support Unity Catalog requires changing the cluster mode to **Single User** or **Shared** and updating the Databricks Runtime version to **11.1** or higher. In this lab, you will need to update the cluster configuration within the workflow multiple times. Detailed instructions are provided in this notebook.
# MAGIC
# MAGIC **Reference:** For best practices on configuring clusters for Unity Catalog, see [Enable a workspace for Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/enable-workspaces.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activities to Perform

# COMMAND ----------

# MAGIC %md
# MAGIC You are expected to carry out the following tasks:
# MAGIC
# MAGIC - **Upgrade Compute Resources**:
# MAGIC   - Update each type of compute resource, including clusters and SQL warehouses, to be compatible with Unity Catalog.
# MAGIC   - Modify cluster configurations to meet Unity Catalog requirements.
# MAGIC - **Adjust Init Scripts** (if applicable):
# MAGIC   - Update or create initialization scripts to support the upgraded environment.
# MAGIC - **Refactor Code**:
# MAGIC   - Update the notebook code to use high-level Spark APIs instead of the lower-level RDD API.
# MAGIC - **Update Workflows**:
# MAGIC   - Modify the code within the workflow notebook.
# MAGIC   - Adjust the workflow's cluster configuration to align with Unity Catalog specifications.
# MAGIC - **Redirect Queries to Unity Catalog**:
# MAGIC   - Implement four different methods to repoint queries from the Hive Metastore to Unity Catalog tables.
# MAGIC
# MAGIC **Important:** Depending on the specifics of the real-world scenario, some of these tasks may require permissions beyond those of a **Workspace Admin**. If you encounter permission issues, coordinate with a **Metastore Admin** or **Account Admin** to proceed. Refer to [Admin privileges in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/admin-privileges.html) for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instructions (Part 1):

# COMMAND ----------

# MAGIC %md
# MAGIC ### Having executed the above `%run` command, you need to upgrade workflow and job cluster from Hive metastore to Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC To do so, you should follow the guidelines below:
# MAGIC
# MAGIC 1. **Update** the **workflow cluster DBR** version to **15.4 LTS**.
# MAGIC 2. **Change** the cluster mode to **Shared**.
# MAGIC 3. **Set** the **default catalog** to a specific catalog on the cluster level.
# MAGIC   - Add the following configuration to the cluster spark settings: **`spark.databricks.sql.initial.catalog.name {name_of_catalog}`** (e.g. `labuser#######_sandbox2`)
# MAGIC 4. **Run the workflow**.
# MAGIC   - The workflow run will fail. _This is expected._
# MAGIC     - The **RDD API** is not supported on shared mode.

# COMMAND ----------

# MAGIC %md
# MAGIC Having completed Part 1, you should now run **Checkpoint 4.1** below (`checkpoint_cluser_configuration_upgrade`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint 4.1: Verifying Cluster Upgrades

# COMMAND ----------

# MAGIC %md
# MAGIC In this checkpoint, you will:
# MAGIC
# MAGIC - **Inspect Cluster Configurations**:
# MAGIC   - Retrieve and examine the cluster settings to ensure they meet Unity Catalog requirements.
# MAGIC
# MAGIC **Note:** Again, depending on the specifics of a real-world scenario, upgrading clusters and workflows may require coordination with users who have elevated privileges, such as **Metastore Admins** or **Cloud Administrators**. If you encounter permission issues, please consult with your customer counterparts performing these roles.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run this checkpoint script before proceeding to Part 2

# COMMAND ----------

# DBTITLE 1,RUN checkpoint_cluster_configuration_upgrade
# MAGIC %run ./Includes/checkpoints/checkpoint_cluster_configuration_upgrade

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Instructions (Part 2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Navigate to the job notebook (`./Includes/assets/notebook_to_upgrade_to_uc`) and begin refactoring the notebook code as follows:
# MAGIC 1. Update the code from the **RDD API** to use the **DataFrame API** instead
# MAGIC 2. Update the code to reference the **Unity Catalog tables** instead of Hive metastore tables
# MAGIC 3. **Re-run the workflow**. 
# MAGIC
# MAGIC If you have done everything correctly, the workflow should run successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Once the workflow job has successfully completed, proceed below to run Checkpoint 4.2 (`checkpoint_workflow_upgrade`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint 4.2: Verifying Notebook and Workflow Upgrades

# COMMAND ----------

# MAGIC %md
# MAGIC In this checkpoint, you will:
# MAGIC
# MAGIC - **Verify Notebook Output**:
# MAGIC   - Confirm that the RDD API notebook outputs data to a table, verifying the table's presence and correctness.
# MAGIC - **Validate Workflow Execution**:
# MAGIC   - Ensure that the workflow runs successfully with the updated configurations.

# COMMAND ----------

# DBTITLE 1,RUN checkpoint_workflow_upgrade
# MAGIC %run ./Includes/checkpoints/checkpoint_workflow_upgrade

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion

# COMMAND ----------

# MAGIC %md
# MAGIC ### By completing this section, you have:
# MAGIC
# MAGIC - Upgraded clusters to be compatible with Unity Catalog.
# MAGIC - Refactored code to use higher-level APIs, enhancing performance and compatibility.
# MAGIC - Updated workflows to point to Unity Catalog tables instead of the Hive Metastore.
# MAGIC - Verified that clusters, notebooks, and workflows are correctly configured and operational.
# MAGIC
# MAGIC These steps are crucial in ensuring a seamless transition to Unity Catalog, leveraging its advanced data governance and security features.
# MAGIC
# MAGIC **References**:
# MAGIC
# MAGIC - [Set up and manage Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html)
# MAGIC - [Enable a workspace for Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/enable-workspaces.html)
# MAGIC - [Admin privileges in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/admin-privileges.html)
# MAGIC - [Upgrade Hive tables and views to Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## You may now proceed to: Lab Teardown

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>