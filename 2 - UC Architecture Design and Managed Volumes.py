# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC <div style ="text-align: center; line-height: 0; padding-top: 9px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 2: Unity Catalog Architecture and Managed Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, you will design the Unity Catalog architecture by creating catalogs, schemas (databases), and volumes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activities to Perform
# MAGIC
# MAGIC You will carry out the following tasks:
# MAGIC
# MAGIC - **Create two catalogs**, specifying the default managed table location where applicable.
# MAGIC - **Create schemas** within the catalogs.
# MAGIC - **Create a managed volume**.
# MAGIC   - Optionally, you may upload data to the volumes for testing purposes, although for grading purposes in this lab, simply creating them will suffice.
# MAGIC
# MAGIC **Note:** In any real-world scenario, some of these tasks may require collaboration with users who have elevated privileges, such as **Metastore Admins** or **Cloud Administrators**, especially when dealing with external storage configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC You will create two catalogs with the following requirements:
# MAGIC
# MAGIC **First Catalog:**
# MAGIC - **Name:** `{userID}_sandbox1`
# MAGIC
# MAGIC **Second Catalog:**
# MAGIC - **Name:** `{userID}_sandbox2`
# MAGIC
# MAGIC - **In both cases, the default Managed Table Location should not be specified** (it will default to the metastore's default location).
# MAGIC
# MAGIC You can create the catalogs either via SQL commands or through the Databricks UI.

# COMMAND ----------

###### YOU ARE EXPECTED TO PERFORM THE STEPS ABOVE, THE FOLLOWING CODE CELLS MAY BE USED TO COMPLETE NOTEBOOK ACTIVITIES. ######

# COMMAND ----------

# DBTITLE 1,def get_username()
# Optionally, use this code cell to retrieve your userID since using your email may not work.

def get_username():
  user_email = spark.sql('select current_user() as user').collect()[0]['user']
  user_id = user_email.split("@")[0]
  return user_id

user_id = get_username()
print(user_id)

# COMMAND ----------

# DBTITLE 1,SQL CREATE CATALOG - Managed External Location (ILLUSTRATION ONLY)
# In a real-world scenario, if you wanted to create a catalog with an external location to store managed tables, you would use the MANAGED LOCATION clause to specify the location in the sql query or via the UI.
# ILLUSTRATION ONLY

# %python
# # Creating the first catalog with a specified managed location
# spark.sql(f"""
#           CREATE CATALOG `{user_id}_sandbox1` 
#           MANAGED LOCATION 's3://my-bucket/my-location/uc_upgrade_lab/dev_sandbox1/managed' -- Update this path as necessary
#           """)

# COMMAND ----------

# DBTITLE 1,Create UC Catalog - sandbox1
# ### CODE GOES HERE ###
# # Creating the first catalog without specifying a managed location

# COMMAND ----------

# DBTITLE 1,Create UC Catalog - sandbox2
# ### CODE GOES HERE ###
# Creating the second catalog without specifying a managed location

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Schemas (Databases)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, you need to create three schemas (databases) with the following requirements:
# MAGIC
# MAGIC **First Schema:**
# MAGIC
# MAGIC - **Name:** `airbnb_bronze_db`
# MAGIC - **Catalog:** Under the `{user_id}_sandbox1` catalog
# MAGIC - **Default Managed Table Location:** Do not specify (it will inherit from the catalog)
# MAGIC
# MAGIC **Second Schema:**
# MAGIC
# MAGIC - **Name:** `airbnb_silver_db`
# MAGIC - **Catalog:** Under the `{user_id}_sandbox1` catalog
# MAGIC - **Default Managed Table Location:** Do not specify
# MAGIC
# MAGIC **Third Schema:**
# MAGIC
# MAGIC - **Name:** `airbnb_gold_db`
# MAGIC - **Catalog:** Under the `{user_id}_sandbox1` catalog
# MAGIC - **Default Managed Table Location:** Do not specify
# MAGIC
# MAGIC **Fourth Schema:**
# MAGIC
# MAGIC - **Name:** `tpch_db`
# MAGIC - **Catalog:** Under the `{user_id}_sandbox2` catalog
# MAGIC - **Default Managed Table Location:** Do not specify
# MAGIC
# MAGIC In addition to using the Python code below, you can create these schemas either via SQL commands or through the Databricks UI.
# MAGIC
# MAGIC **Note:** Creating schemas may require certain privileges. Ensure you have the necessary permissions or work with a **Metastore Admin** if needed. Refer to [Manage Unity Catalog object ownership](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/ownership.html) for guidance.

# COMMAND ----------

###### YOU ARE EXPECTED TO PERFORM THE STEPS ABOVE, THE FOLLOWING CODE CELLS MAY BE USED TO COMPLETE NOTEBOOK ACTIVITIES. ######

# COMMAND ----------

# DBTITLE 1,Create sandbox1 Schemas (4)
# ### CODE GOES HERE ###
# Database 1: Creating the airbnb_bronze_db in the user's sandbox1 catalog


# ### CODE GOES HERE ###
# Database 2: Creating the airbnb_silver_db in the user's sandbox1 catalog


# ### CODE GOES HERE ###
# Database 3: Creating the airbnb_gold_db in the user's sandbox1 catalog


# ILLUSTRATION ONLY
# # Database 4: Creating the tpch_db in the user's sandbox2 catalog with a specified managed location
# catalog_name = user_id + "_sandbox2"
# db_name = "tpch_db"
# spark.sql(f"""create database if not exists {catalog_name}.{db_name}
#           managed location 's3://my-bucket/my-location/uc_upgrade_lab/dev_sandbox2/tpch'
#           """)


# ### CODE GOES HERE ###
# Database 4: Creating the tpch_db in the user's sandbox2 catalog without specifying a managed location

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating a Managed Volume

# COMMAND ----------

# MAGIC %md
# MAGIC With the requisite privileges in a real-world environment, if you wanted to create an External Volume, you would just add the `LOCATION` clause to the SQL command upon volume creation.
# MAGIC
# MAGIC For example (from the Databricks docs):
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME catalog_name.schema_name.volume_name 
# MAGIC
# MAGIC LOCATION 's3://my-bucket/my-location/my-path'
# MAGIC
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC As you do not have an S3 bucket or the privileges required to effectively use it in the context of a Unity Catalog upgrade, you cannot create an External Volume, but you can create a **Managed** Volume and test for its existence.
# MAGIC
# MAGIC In this lab environment, you can create a Managed Volume under:
# MAGIC - `{user_id}_sandbox2` catalog
# MAGIC - `tpch_db` database
# MAGIC - name the volume: `managed_volume`

# COMMAND ----------

# DBTITLE 1,Create sandbox2.tpch_db managed volume
# ### CODE GOES HERE ###
# Create a volume in the specified database within the user's sandbox2 catalog

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Checkpoint 2: Verifying Unity Catalog Architecture—External Locations and Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activities to Verify
# MAGIC
# MAGIC The checkpoint script below will assess the following:
# MAGIC
# MAGIC - **Existence of Catalogs:** Verify that the catalogs have been created correctly. [Catalogs API Documentation](https://docs.databricks.com/api/workspace/catalogs)
# MAGIC - **Catalog Default Locations:** Check if the catalogs have the specified default storage locations.
# MAGIC - **Existence of Schemas:** Confirm that the schemas exist within the correct catalogs and have appropriate settings.
# MAGIC - **Permissions and Grants:** Ensure that permissions and grants have been applied correctly to the catalogs and schemas.
# MAGIC - **Existence of Volumes:** Verify the creation of managed and external volumes. [Volumes API Documentation](https://docs.databricks.com/api/workspace/volumes)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Before continuing to the next section, be sure to run the checkpoint script below.

# COMMAND ----------

# DBTITLE 1,RUN checkpoint_uc_architecture_design
# MAGIC %run ./Includes/checkpoints/checkpoint_uc_architecture_design

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion

# COMMAND ----------

# MAGIC %md
# MAGIC By completing this section, you have:
# MAGIC
# MAGIC - Created two (Unity Catalog) catalogs, specifying the default managed table location where applicable.
# MAGIC - Created schemas within your new catalogs (and not within the `hive_metastore` catalog).
# MAGIC - Created a managed volume in one of your new catalogs.
# MAGIC
# MAGIC Successful completion of these tasks prepares your for the ensuing tasks of table and view migration in the next section of the assessment. Remember to verify each migration step to ensure data integrity and consistency.
# MAGIC
# MAGIC **References:**
# MAGIC
# MAGIC - [Unity Catalog best practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)
# MAGIC - [What is Catalog Explorer?](https://docs.databricks.com/en/catalog-explorer/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## You may now proceed to: 3 - Table and View Migration

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>