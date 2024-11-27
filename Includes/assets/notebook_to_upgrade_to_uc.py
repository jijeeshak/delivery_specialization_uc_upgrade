# Databricks notebook source
# MAGIC %md
# MAGIC <div style ="text-align: center; line-height: 0; padding-top: 9px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Notebook to Upgrade to UC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## The following admin setup steps have been performed in preparation for your upgrade activities

# COMMAND ----------

# MAGIC %md
# MAGIC **- Create a cluster with the following configs for the user**
# MAGIC   - No isolation shared
# MAGIC   - Photon disabled
# MAGIC   - 10.4 LTS DBR
# MAGIC   - 1 worker
# MAGIC   - 20 minute termination
# MAGIC   - No instance profile
# MAGIC
# MAGIC **- Create a job with the above cluster and this notebook**

# COMMAND ----------

# MAGIC %md
# MAGIC **Note: To upgrade clusters to Unity Catalog, you need to choose `Single User` or `Shared` mode and update the DBR version to version 11.1 or above. In this part of the lab, you will be required to update the job cluster driving the task in the workflow a multiple times. See the instructions in this notebook for details.**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read the TPCH `lineitem` table from HMS

# COMMAND ----------

# DBTITLE 1,get user_id and USE hive_metastore
def get_username():
  # Execute a SQL query to get the current user's email
  user_email = spark.sql('select current_user() as user').collect()[0]['user']
  # Split the email on '@' and take the first part as the user ID
  user_id = user_email.split("@")[0]
  return user_id

# Call the function to get the user ID
user_id = get_username()
# Display the user ID
print(user_id)

# Set the current catalog to hive_metastore
spark.sql("USE CATALOG hive_metastore")

# COMMAND ----------

# DBTITLE 1,read tpch_db.lineitem
# Display the contents of the "lineitem" table from the user-specific TPCH database
# The table name is dynamically constructed using the user's ID
display(spark.read.table(f"{user_id}_tpch_db.lineitem"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Since we are still using the two-level namespace, Databricks will read from the `hive_metastore` catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC _We can confirm this like so (line 23 in the result):_

# COMMAND ----------

# DBTITLE 1,describe tpch_db.lineitem
# Display the schema and metadata of the "lineitem" table in the user-specific TPCH database
# The table name is dynamically constructed using the user's ID
display(spark.sql(f"""
          describe table extended {user_id}_tpch_db.lineitem
          """
          )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _This notebook is fairly simple/straightforward **but** it is quite old and was never updated to fully use the DataFrame API._

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Instructions (Part 2):

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implement the following refactoring steps :
# MAGIC 1. Update the code from the **RDD API** to use the **DataFrame API** instead
# MAGIC 2. Update the code to reference the Unity Catalog tables instead of Hive metastore tables
# MAGIC 3. Re-run the workflow. 
# MAGIC
# MAGIC #### If you have done everything correctly, the workflow should run successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Before proceeding, be sure to run **Checkpoint 4.1** in the referring notebook

# COMMAND ----------

###### YOU ARE EXPECTED TO PERFORM THE STEPS ABOVE, THE FOLLOWING CODE CELLS MAY BE REFACTORED TO COMPLETE NOTEBOOK ACTIVITIES. ######

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### First, create a new column with a string representing a reference timestamp and a column with a unique hash for each row in the table.

# COMMAND ----------

# DBTITLE 1,create input_df
input_df = spark.read.table(f"{user_id}_tpch_db.lineitem").limit(1000) # Limiting to keep dataset size small 

# COMMAND ----------

# DBTITLE 1,create hashed_df via RDD API
hashed_df = (input_df.rdd
    .map(lambda x: x + ('1970-01-01 00:00:00.000',))  # Append a fixed reference timestamp to each row
    .map(lambda x: x + (hash(str(x)),))  # Append a hash of the row (including the timestamp) to each row
    .toDF(input_df.columns + ["reference_timestamp", "hash"])  # Convert back to DataFrame with added columns
)
display(hashed_df)  # Display the DataFrame with the added reference timestamp and hash columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Then, get the maximum `l_quantity` for each ship date

# COMMAND ----------

# DBTITLE 1,create max_quantity_df via RDD API
# Convert DataFrame to RDD
rdd = hashed_df.rdd.map(lambda row: (row["l_shipdate"], row["l_quantity"]))

# Group by key (l_shipdate) and find the maximum value of l_quantity
grouped_rdd = rdd.groupByKey().mapValues(lambda quantities: max(quantities))

# Convert the RDD back to a DataFrame
max_quantity_df = grouped_rdd.toDF(["l_shipdate", "max_l_quantity"])

# Display the DataFrame
display(max_quantity_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Finally, we are interested in the orders table for urgent orders after a certain date

# COMMAND ----------

# DBTITLE 1,display urgent recent tpch_db.orders data
display(
    spark.sql(
        f""" 
          SELECT * 
          FROM {user_id}_tpch_db.orders
          WHERE o_orderpriority = '1-URGENT'
          AND o_orderdate >= '1997-07-01'
          """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Once you have refactored the code above according to our migration requirements, be sure to run **Checkpoint 4.2** in the referring notebook

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>