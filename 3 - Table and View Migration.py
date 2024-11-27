# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC <div style ="text-align: center; line-height: 0; padding-top: 9px">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 3: Table and View Migration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SETUP: IMMEDIATELY RUN THE MANAGED TABLES SETUP CELL BELOW

# COMMAND ----------

# MAGIC %run ./Includes/setup/managed_tables_setup

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, you will focus on migrating tables and views from the Hive metastore to Unity Catalog. In a real-world scenario, the migration process typically involves upgrading entire schemas as well as individual tables, handling both managed and external tables, and exploring different methods and scenarios. For the purposes of our lab, you will focus on upgrading managed tables and views to Unity Catalog.
# MAGIC
# MAGIC **Note:** While the migration can be performed using the Databricks UI or SQL commands, this lab section will utilize and speak to the use of SQL commands for consistency and clarity. The UI approach is generally more straightforward and can be explored independently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activities to Perform
# MAGIC
# MAGIC You will undertake the following tasks:
# MAGIC
# MAGIC - **Upgrade an entire schema** from the Hive metastore to Unity Catalog.
# MAGIC - **Upgrade individual tables** in another schema from managed tables in the Hive metastore to managed tables in Unity Catalog.
# MAGIC - **Upgrade a view** from the Hive metastore to Unity Catalog.
# MAGIC
# MAGIC These activities will expose you to various migration methods, including:
# MAGIC
# MAGIC - Utilizing `CREATE TABLE AS SELECT (CTAS)` and `DEEP CLONE` for table migration.
# MAGIC - Recreating views using the Unity Catalog three-level namespace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview of Migration Scenarios
# MAGIC
# MAGIC A number of migration scenarios, _not all of which you are able to perform in this lab_, are summarized as follows.
# MAGIC
# MAGIC Please note that tasks pertaining to external tables will be referenced for illustrative purposes only:
# MAGIC
# MAGIC 1. _**Upgrade the entire schema `hive_metastore.user#######_airbnb_bronze_db`**:_
# MAGIC    - **Scenario**: External tables to external tables.
# MAGIC
# MAGIC 2. _**Upgrade individual tables in `hive_metastore.user#######_airbnb_silver_db`**:_
# MAGIC    - **Table 1 (`listings_silver`)**: External to external.
# MAGIC    - **Table 2 (`neighbourhoods_silver`)**: Managed to external.
# MAGIC    - **Table 3 (`reviews_silver`)**: External to managed.
# MAGIC
# MAGIC 3. **Upgrade individual tables in `hive_metastore.user#######_tpch_db`**:
# MAGIC    - **Scenario**: Managed tables to managed tables.
# MAGIC
# MAGIC By covering these scenarios, you will gain perspective on and experience with:
# MAGIC
# MAGIC - **External to external** table migration.
# MAGIC - **Managed to managed** table migration.
# MAGIC - **Managed to external** table conversion.
# MAGIC - **External to managed** table conversion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating an Entire Schema: External to External

# COMMAND ----------

# MAGIC %md
# MAGIC ### _For illustrative purposes only_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task: Upgrade `hive_metastore.user#######_airbnb_bronze_db` to Unity Catalog
# MAGIC
# MAGIC Suppose you want to migrate the entire schema `user#######_airbnb_bronze_db` from the Hive metastore to Unity Catalog, maintaining all tables as (hypothetical) external tables.
# MAGIC
# MAGIC **Note:** Migrating an entire schema may require elevated privileges. Ensure you have the necessary permissions or work with a **Metastore Admin** to proceed.
# MAGIC
# MAGIC First, perform a dry run to preview the migration:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC SYNC SCHEMA user#######_sandbox1.airbnb_bronze_db FROM hive_metastore.user#######_airbnb_bronze_db DRY RUN;
# MAGIC
# MAGIC
# MAGIC ````

# COMMAND ----------

# MAGIC %md
# MAGIC Then, execute the migration:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC SYNC SCHEMA user#######_sandbox1.airbnb_bronze_db FROM hive_metastore.user#######_airbnb_bronze_db;
# MAGIC
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Reference:** For more details on the `SYNC` command, see [Upgrade Hive tables and views to Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Migrating Individual Tables in `airbnb_silver_db`

# COMMAND ----------

# MAGIC %md
# MAGIC ### _For illustrative purposes only_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: External Table to External Table
# MAGIC
# MAGIC **Task:** Upgrade `hive_metastore.user#######_airbnb_silver_db.listings_silver` to Unity Catalog as an external table.
# MAGIC
# MAGIC This task requires the execution of the following command:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC SYNC TABLE user#######_sandbox1.airbnb_silver_db.listings_silver FROM hive_metastore.user#######_airbnb_silver_db.listings_silver;
# MAGIC
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Ensure you have the necessary permissions to perform this operation. You may need **SELECT** privileges on the source table and **CREATE** privileges on the target schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: Managed Table to External Table
# MAGIC
# MAGIC **Task:** Upgrade `hive_metastore.user#######_airbnb_silver_db.neighbourhoods_silver` to Unity Catalog, converting it from a managed table to an external table.
# MAGIC
# MAGIC Since you are converting a managed table to an external table, you need to specify a `LOCATION` clause. You can use either `DEEP CLONE` or `CREATE TABLE AS SELECT (CTAS)` for this purpose.

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 1: Using `DEEP CLONE`**

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE user#######_sandbox1.airbnb_silver_db.neighbourhoods_silver
# MAGIC
# MAGIC DEEP CLONE hive_metastore.user#######_airbnb_silver_db.neighbourhoods_silver
# MAGIC
# MAGIC LOCATION 's3://my-bucket/my-location/uc_upgrade_lab/dev_sandbox1/user/user#######/airbnb_silver/neighbourhoods';
# MAGIC
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 2: Using `CTAS`**

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC CREATE TABLE user#######_sandbox1.airbnb_silver_db.neighbourhoods_silver
# MAGIC
# MAGIC LOCATION 's3://my-bucket/my-location/uc_upgrade_lab/dev_sandbox1/user/user#######/airbnb_silver/neighbourhoods'
# MAGIC
# MAGIC AS SELECT * FROM hive_metastore.user#######_airbnb_silver_db.neighbourhoods_silver;
# MAGIC
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Replace the `LOCATION` path with the appropriate external location accessible to your Unity Catalog metastore.
# MAGIC
# MAGIC **Important:** Verify that the resulting table is an external table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: External Table to Managed Table
# MAGIC
# MAGIC **Task:** Upgrade `hive_metastore.user#######_airbnb_silver_db.reviews_silver` to Unity Catalog, converting it from an external table to a managed table.
# MAGIC
# MAGIC Use the `CREATE TABLE AS SELECT (CTAS)` method:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC
# MAGIC CREATE TABLE user#######_sandbox1.airbnb_silver_db.reviews_silver
# MAGIC
# MAGIC AS SELECT * FROM hive_metastore.user#######_airbnb_silver_db.reviews_silver;
# MAGIC
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** This command creates a managed table in Unity Catalog by copying data from the external table in the Hive metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating Individual Tables in `tpch_db`: Managed to Managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task: Upgrade tables in `hive_metastore.{user_id}_tpch_db` to Unity Catalog
# MAGIC
# MAGIC Having considered the hypothetical scenarios outlined above, you will migrate the following tables, maintaining them as managed tables:
# MAGIC
# MAGIC - `orders`
# MAGIC - `lineitem`
# MAGIC - `region`
# MAGIC
# MAGIC Use the `CREATE TABLE AS SELECT (CTAS)` method for each table:

# COMMAND ----------

###### YOU ARE EXPECTED TO PERFORM THE STEPS ABOVE, THE FOLLOWING CODE CELLS MAY BE USED TO COMPLETE NOTEBOOK ACTIVITIES. ######

# COMMAND ----------

# DBTITLE 1,def get_username()
def get_username():
  # Retrieve the current user's email from the Spark SQL context
  user_email = spark.sql('select current_user() as user').collect()[0]['user']
  # Extract the user ID from the email by splitting at the '@' character
  user_id = user_email.split("@")[0]
  return user_id

# Get the user ID by calling the get_username function
user_id = get_username()
# Print the user ID to the console
print(user_id)

# COMMAND ----------

# DBTITLE 1,migrate tpch_db.orders
# MAGIC %sql
# MAGIC
# MAGIC -- CODE GOES HERE
# MAGIC -- Migrate 'orders' table
# MAGIC

# COMMAND ----------

# DBTITLE 1,migrate tpch_db.lineitem
# MAGIC %sql
# MAGIC
# MAGIC -- CODE GOES HERE
# MAGIC -- Migrate 'lineitem' table
# MAGIC

# COMMAND ----------

# DBTITLE 1,migrate tpch_db.region
# MAGIC %sql
# MAGIC
# MAGIC -- CODE GOES HERE
# MAGIC -- Migrate 'region' table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Reference:** For best practices on upgrading managed tables, see [Upgrade Hive tables and views to Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html#upgrade-managed-tables).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upgrading a View in `airbnb_gold_db`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task: Upgrade the view `hive_metastore.{user_id}_airbnb_gold_db.vw_property` to Unity Catalog
# MAGIC
# MAGIC Upgrading a view requires _**recreating it**_ in Unity Catalog using the three-level namespace.

# COMMAND ----------

# MAGIC %md
# MAGIC #### First, retrieve the view definition:

# COMMAND ----------

# Print the user ID to the console
print(user_id)

# COMMAND ----------

# Display the result of the SQL query that shows the CREATE TABLE statement for the specified view
display(
    spark.sql(
        f"""
          SHOW CREATE TABLE hive_metastore.{user_id}_airbnb_gold_db.vw_property
          """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Copy the view definition, excluding any `TBLPROPERTIES`, and update all table references to use the Unity Catalog three-level namespace (`catalog.schema.table`).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recreate the view in Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CODE GOES HERE
# MAGIC -- Create a view named 'vw_property' in the 'airbnb_gold_db' database for user 'labuser#######_sandbox1'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Ensure all table references within the view use the Unity Catalog three-level namespace.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify the view:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CODE GOES HERE
# MAGIC -- Select all columns from the 'vw_property' view in the 'airbnb_gold_db' database
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Reference:** For more information on upgrading views, see [Upgrade Hive tables and views to Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html#upgrade-views).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint 3: Verifying Table and View Migration

# COMMAND ----------

# MAGIC %md
# MAGIC In this checkpoint, you will assess whether the executable migration activities above were completed successfully.
# MAGIC
# MAGIC ### Activities to Verify
# MAGIC
# MAGIC - **Table Existence**: Confirm that each table has been migrated to Unity Catalog.
# MAGIC - **Table Type**: Verify whether the tables are managed or external as intended.
# MAGIC - **Data Integrity**: Check that the number of records matches between the source and migrated tables.
# MAGIC - **View Existence**: Ensure the view has been recreated in Unity Catalog.
# MAGIC - **View Correctness**: Validate the view returns the expected results.

# COMMAND ----------

# MAGIC %md
# MAGIC **IMPORTANT:** Verification requires appropriate permissions to access the Unity Catalog tables and views. To that end, in order for the service principal (`dbsp`) that will ultimately run your assessment grading to be able to give you full credit for the activities assessed in this section, you **MUST** first grant the service principal the **Data Reader Privilege preset** on both catalogs that you have created so far in the lab. This is done most easily from the **Catalog Explorer**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Once the requisite privileges outlined above have been granted to the service principal, run the checkpoint script below.

# COMMAND ----------

# DBTITLE 1,RUN checkpoint_table_and_view_migration
# MAGIC %run ./Includes/checkpoints/checkpoint_table_and_view_migration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion

# COMMAND ----------

# MAGIC %md
# MAGIC By completing this section, you have:
# MAGIC
# MAGIC - Migrated an entire schema from the Hive metastore to Unity Catalog.
# MAGIC - Considered the migration of individual tables across multiple hypothetical scenarios, such as external to external, managed to external, and external to managed.
# MAGIC - Migrated managed tables in a schema, keeping them as managed tables in Unity Catalog.
# MAGIC - Upgraded a view by recreating it using the Unity Catalog three-level namespace.
# MAGIC
# MAGIC These hand-on activities have equipped you with a practical understanding of how to migrate various data assets to Unity Catalog, preparing you for real-world upgrade scenarios.
# MAGIC
# MAGIC **References:**
# MAGIC
# MAGIC - [Upgrade Hive tables and views to Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html)
# MAGIC - [Admin privileges in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/admin-privileges.html)
# MAGIC - [Manage privileges in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## You may now proceed to: 4 - Cluster, Notebook, and Workflow Upgrades

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>