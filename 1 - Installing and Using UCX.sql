-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div style ="text-align: center; line-height: 0; padding-top: 9px">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1 - Installing and Using UCX

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📌 Notice
-- MAGIC
-- MAGIC #### THIS NOTEBOOK IS INTENDED TO TO PROVIDE YOU WITH INSIGHT INTO NON-STANDARD OPTIONS FOR INSTALLING UCX. 
-- MAGIC
-- MAGIC #### **IT DOES NOT CONTAIN EXECUTABLE CELLS OR GRADED ACTIVITIES.**
-- MAGIC
-- MAGIC #### _TO PROCEED IMMEDIATELY TO GRADED ACTIVITIES, PROCEED TO NOTEBOOK 2._

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What is UCX?
-- MAGIC
-- MAGIC In this section of the assessment, we will discuss key aspects of the installation and configuration of the **Unity Catalog Accelerator (UCX)** on your Databricks workspace.
-- MAGIC
-- MAGIC UCX is a suite of utilities designed to simplify and automate the process of upgrading to [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html). 
-- MAGIC
-- MAGIC We will cover the prerequisites for installing UCX, including the setup of authentication and the preparation of your compute environment, then walk you through a UCX installation that gives you a sense of the customization options of the UCX suite.
-- MAGIC
-- MAGIC Additionally, we will explore the execution assessment workflows to evaluate the compatibility of your workspace entities with Unity Catalog, ensuring a smooth and efficient upgrade process.
-- MAGIC
-- MAGIC Please note that this summary draws heavily from the [Installing and Using UCX Demo](https://partner-academy.databricks.com/learn/course/2195/play/15656/installing-and-using-ucx-demo) video in the Partner Academy course, which you may refer to for a step-by-step document of the UCX installation and assessment workflow execution process. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC Before beginning any UCX installation, ensure that you have the following:
-- MAGIC
-- MAGIC - A **Databricks all-purpose single-user cluster** using **Databricks Runtime (DBR) version 15.1** or later.
-- MAGIC - The **Web Terminal** application installed, for interactive command-line interface (CLI) access.
-- MAGIC - **Databricks CLI** installed (version **0.219.0** or newer).
-- MAGIC - Access to the **API credentials** for the target workspace you intend to upgrade (workspace URL and personal access token). The user associated with these credentials must have **workspace admin** privileges.
-- MAGIC
-- MAGIC **Note:** Certain steps may require collaboration with users who have elevated privileges, such as **Account Admins** or **Metastore Admins**, especially when configuring account-level settings or accessing shared resources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Configuring Your Cluster for Installation of UCX at the User Level
-- MAGIC
-- MAGIC Before starting your cluster, you might have the the need to configure the environment variables of the driver node to force a user-level installation of UCX. This is a straightforward process.
-- MAGIC
-- MAGIC - Click **Compute** and select the cluster by it Name
-- MAGIC - Confirm that the cluster is configured for use of **Databricks Runtime (DBR) version 15.1** or later.
-- MAGIC - Click the **Edit** button from the upper right hand corner of the cluster page.
-- MAGIC - At the bottom of the page, click **Advanced options**
-- MAGIC - In the Environment Variables input box, enter the following environment variable: `UCX_FORCE_INSTALL=user`
-- MAGIC   - This non-default setting offers the benefit of installing UCX at the level of the user, in a `/.ucx` subfolder of the user folder. This can be particularly helpful in scenarios where you want to generate a Unity Catalog migration readiness assessment without overwriting a global installation of UCX (`/Workspaces/Applications/UCX`).
-- MAGIC - Click **Confirm** at the bottom of the page to save your edits.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Launching the Databricks CLI in the Web Terminal
-- MAGIC
-- MAGIC To access the Databricks CLI on your compute cluster, follow these steps:
-- MAGIC
-- MAGIC 1. In the left sidebar of your Databricks workspace, right-click on **Compute** and select **Open Link in New Tab**.
-- MAGIC 2. In the new tab, select your Databricks compute cluster (which should be running DBR 15.1 or later).
-- MAGIC 3. Navigate to the **Apps** tab.
-- MAGIC 4. Click on the **Web Terminal** button.
-- MAGIC
-- MAGIC Your Web Terminal should now be open in a new browser tab, providing you with interactive CLI access to your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuring Databricks Authentication (if necessary)
-- MAGIC
-- MAGIC **The steps in this cell may be omitted if:**
-- MAGIC
-- MAGIC - Your compute environment is a Databricks cluster (DBR 15.1 or later), **and**
-- MAGIC - You are targeting the same workspace from which the cluster was created.
-- MAGIC
-- MAGIC To configure authentication with the target workspace, run the following command in your Web Terminal:
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC databricks configure
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC When prompted:
-- MAGIC
-- MAGIC - Enter the **workspace URL** (only the hostname part, e.g., `https://your-workspace-url`).
-- MAGIC - Provide a **personal access token (PAT)** associated with a workspace admin user (must be a user account, not a service principal).
-- MAGIC
-- MAGIC **Important:** Configuring authentication requires **workspace admin** privileges. If you do not have these privileges, you will need to work with a user who does.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prepare the Compute Environment
-- MAGIC
-- MAGIC If you are using a Databricks cluster, execute the following commands in your Web Terminal before proceeding:
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC apt-get update
-- MAGIC
-- MAGIC apt install -y python3.10-venv
-- MAGIC
-- MAGIC unset DATABRICKS_RUNTIME_VERSION
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC These commands:
-- MAGIC
-- MAGIC - Update the package lists for upgrades and new packages.
-- MAGIC - Install the Python 3.10 virtual environment package.
-- MAGIC - Unset the `DATABRICKS_RUNTIME_VERSION` environment variable to prevent version conflicts.
-- MAGIC
-- MAGIC **Note:** If you are using a different compute environment, additional setup steps may be required, which are beyond the scope of this lab.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Install and Configure UCX
-- MAGIC
-- MAGIC To install and configure UCX—the collection of assessment and migration tools for upgrading to Unity Catalog—run the following command in your Web Terminal:
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC databricks labs install ucx
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC **Note**: Should you encounter an upfront blocker to the installation with the following message:
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC Installing the CLI..
-- MAGIC
-- MAGIC Target path /usr/local/bin/databricks already exists.
-- MAGIC
-- MAGIC If you have an existing Databricks CLI installation, please first remove it using
-- MAGIC
-- MAGIC   sudo rm '/usr/local/bin/databricks'
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC You will want to:
-- MAGIC
-- MAGIC 1. Execute the suggested `sudo rm` command.
-- MAGIC 2. Reinstall the Databricks CLI to the driver node using curl, as follows:
-- MAGIC   - `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
-- MAGIC 3. Re-run `databricks labs install ucx`
-- MAGIC
-- MAGIC During the installation, you will be prompted with several configuration options. For the purpose of this discussion, we assume that you **accept the default options** (or select the first option, when required), **except for the following prompts**:
-- MAGIC
-- MAGIC **Inventory Database stored in the hive metastore**: `ucx_{user_id}`
-- MAGIC
-- MAGIC **Important:**
-- MAGIC
-- MAGIC - UCX can be configured to traverse additional workspaces within your organization. However, this requires an **Account Admin** to obtain your Databricks **Account ID** and set up a service principal with the necessary permissions.
-- MAGIC - Since, as a lab user, you do not have sufficient **workspace admin** privileges, to say nothing of account-level privileges, your lab activities will not include hands-on work with the UCX suite.
-- MAGIC
-- MAGIC Even so, after completing a successful initial configuration, it's good to know that UCX will create several artifacts in your target workspace:
-- MAGIC
-- MAGIC - **Configuration Files and README:** Located in `/{username}/.ucx`, these files contain the settings and additional information about UCX. Refer to the README file for detailed instructions.
-- MAGIC - **Workflows:** A collection of workflows that perform various tasks associated with the upgrade process.
-- MAGIC - **Assessment Dashboard:** A dashboard that displays the results of the assessment phase after running the assessment workflow.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Executing the Assessment Workflow
-- MAGIC
-- MAGIC The assessment workflow is crucial as it evaluates the compatibility of your workspace entities with Unity Catalog. It identifies incompatible entities and provides essential information for planning your upgrade.
-- MAGIC
-- MAGIC There are three methods to trigger the assessment:
-- MAGIC
-- MAGIC 1. **Automatically during UCX Configuration:** If you selected this option during the UCX installation, the assessment will run automatically.
-- MAGIC 2. **Using the Databricks CLI:** Run the following command in your Web Terminal:
-- MAGIC
-- MAGIC    ```
-- MAGIC    databricks labs ucx ensure-assessment-run
-- MAGIC    ```
-- MAGIC
-- MAGIC 3. **Through the Workspace UI:** Manually trigger the workflow by navigating to the workspace's user interface and running the assessment workflow interactively.
-- MAGIC
-- MAGIC The output of each task within the assessment workflow is stored as tables in a database schema you specified during the UCX configuration.
-- MAGIC
-- MAGIC **Note:** Executing the assessment may require access privileges to read metadata about workspace entities. Ensure your user account has the necessary permissions or collaborate with a **Metastore Admin** if required.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viewing and Interpreting the Assessment Results
-- MAGIC
-- MAGIC The results of the assessment are compiled into a dashboard that provides insights into potential challenges and the overall readiness of your workspace for the upgrade.
-- MAGIC
-- MAGIC To access the dashboard:
-- MAGIC
-- MAGIC 1. In the left sidebar of your Databricks workspace, click on **Dashboards**.
-- MAGIC 2. Navigate to the **Legacy Dashboards** tab.
-- MAGIC 3. Select **UCX Assessment (Main)**.
-- MAGIC
-- MAGIC The dashboard presents an inventory of assets, highlighting any compatibility issues or concerns. Use this information to:
-- MAGIC
-- MAGIC - **Identify potential challenges.**
-- MAGIC - **Prioritize your upgrade efforts.**
-- MAGIC - **Develop a remediation plan to address any issues.**
-- MAGIC
-- MAGIC **Tip:** Interpreting the results may require a deep understanding of your workspace's data assets and dependencies. Collaborate with relevant stakeholders, such as data engineers or **Metastore Admins**, to address identified issues.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC In this portion of the lab, you have explored some of the nuance of installing UCX on your Databricks workspace as preparation for your Unity Catalog migration. With UCX installed, you will be equipped to run the assessment workflow and evaluate the compatibility of your workspace entities with Unity Catalog. By interpreting the results presented in the assessment dashboard, you can more easily identify potential compatibility issues and plan your upgrade strategy accordingly. This proactive approach will help you prioritize remediation efforts and facilitate a smooth transition to Unity Catalog.
-- MAGIC
-- MAGIC **Remember:** Upgrading to Unity Catalog _**will**_ require collaboration with users who have elevated privileges, such as **Account Admins**, **Metastore Admins**, or **Cloud Administrators** (e.g., AWS S3 admins). Engaging with these stakeholders early in the process will help address dependencies and ensure a successful upgrade.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## You may now proceed to: 2 - Unity Catalog Architecture

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>