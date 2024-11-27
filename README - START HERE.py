# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC <div style ="text-align: center; line-height: 0; padding-top: 9px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delivery Specialization - Unity Catalog Upgrade - Lab Assessment

# COMMAND ----------

# MAGIC %md
# MAGIC ##SETUP: IMMEDIATELY RUN THE USER SETUP CELL BELOW
# MAGIC The referenced include takes approximately five (5) minutes to complete, on top of the 5 minutes it will take to start your all-purpose compute cluster, so be sure to execute it promptly.

# COMMAND ----------

# DBTITLE 1,RUN user_setup
# MAGIC %run ./Includes/setup/user_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## WELCOME

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations on your successful completion of the Partner Academy's **[Delivery Specialization: UC Upgrade](https://partner-academy.databricks.com/learn/course/2195/delivery-specialization-uc-upgrade)** course!
# MAGIC
# MAGIC This lab assessment is designed to complement the knowledge check from the end of the course with hands-on activities touching on various project tasks sampled from **[Upgrade Plans at all three levels of complexity](https://partner-academy.databricks.com/learn/course/2195/play/24068/upgrade-plans?hash=4130e87e4f44f34e946aaa0b3ca327f4aebfb973&generated_by=295717)**, and in the manner of an **[In-place](https://partner-academy.databricks.com/learn/course/2195/play/24069/migration-options?hash=4130e87e4f44f34e946aaa0b3ca327f4aebfb973&generated_by=295717)** migration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The graded agenda of the assessment is organized into three sections:
# MAGIC
# MAGIC #### - Unity Catalog Architecture Design
# MAGIC #### - Table and View Migration
# MAGIC #### - Cluster, Notebook, and Workflow Upgrades

# COMMAND ----------

# MAGIC %md
# MAGIC In order to complete the lab assessment, you will be expected to apply your domain expertise in Unity Catalog migration as well as your technical acumen, for which the recommended minimum certification is **[Databricks Certified Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONTEXT AND EXPECTATION SETTING

# COMMAND ----------

# MAGIC %md
# MAGIC This lab will not assess your ability to execute Unity Catalog migration tasks requiring administrator privileges. As a reminder from the course, **the successful execution of a Unity Catalog migration relies on the cooperation and teamwork of a number of key administrator stakeholders**, namely:
# MAGIC
# MAGIC - **Account admins**
# MAGIC - **IdP admins**
# MAGIC - **Cloud admins**
# MAGIC - **Workspace admins**
# MAGIC - **Metastore admins**
# MAGIC
# MAGIC _With that said, for the purposes of this lab, you will be adopting the persona of a Data Engineer without admin privileges of any kind._

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACTIVITY AGENDA

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Unity Catalog Architecture Design
# MAGIC   - Creating Catalogs
# MAGIC   - Creating Schemas (Databases)
# MAGIC   - Creating a Managed Volume
# MAGIC ### 2. Table and View Migration
# MAGIC   - Upgrading an entire schema
# MAGIC   - Upgrading individual managed tables
# MAGIC   - Upgrading a view
# MAGIC ### 3. Cluster, Notebook, and Workflow Upgrades
# MAGIC   - Upgrading Compute Resources
# MAGIC   - Adjusting Init Scripts (as applicable)
# MAGIC   - Refactoring Code
# MAGIC   - Updating Workflows
# MAGIC   - Redirecting Queries to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## KEYS TO A SUCCESSFUL LAB

# COMMAND ----------

# MAGIC %md
# MAGIC We encourage you to manage your time effectively in this lab, remaining mindful that at a number of junctures over the course of your lab activities, you will be initializing operations and processes that consume time (like the `user_setup` script you initialized at the top of this notebook). The lab itself is capped at a maximum of two (2) attempts, with each attempt being capped for time at two (2) hours.
# MAGIC
# MAGIC ###The following points in your lab progression stand to have the greatest impact on effective time management:
# MAGIC
# MAGIC - **Execution of setup and teardown scripts**
# MAGIC - **Starting (and/or restarting) of all-purpose and job clusters**
# MAGIC - **Execution of checkpoint scripts (which drive your assessment grade)**
# MAGIC - **Execution of workflow job runs**
# MAGIC - **Execution of grading job runs**
# MAGIC
# MAGIC **_Lastly, as a general note, we want to stress to you that the activity instructions of this assessment should be adhered to with the utmost care and attention paid to their specifics._**
# MAGIC
# MAGIC Your ability to not only create a working solution but also precisely design for and implement the exact migration tasks on the agenda is the ultimate basis of your grade. **Resist any temptation to deviate from the specific instructions of each assessment section.**
# MAGIC
# MAGIC A number of the activities to be performed in this lab can be completed in three different ways, either via **Python**, **SQL**, or through the appropriate interfaces in the **Databricks UI**. We suggest that you play to your unique strengths as a Databricks practitioner and Unity Catalog subject matter expert, but we also encourage you to consider the benefits of reusable code and any automations that might be particularly time-saving.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best wishes for a successful assessment!

# COMMAND ----------

# MAGIC %md
# MAGIC Provided you have successfully run the `user_setup` include at the top of this notebook and your all-purpose compute cluster is still running, you may proceed to the first section of the agenda, on **Databricks Labs UCX**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>

# COMMAND ----------

# MAGIC %md
# MAGIC