# Databricks notebook source
dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtracking_load_bronze_setfullload",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtracking_load_silver_setfullload",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtracking_load_gold_setfullload",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtrackingdw_load_bronze_tables",300)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtrackingdw_load_silver_tables",300)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtrackingdw_load_gold_dimension",300)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/WebTrackingDW/load/webtrackingdw_load_gold_facts",300)

# COMMAND ----------

dbutils.notebook.exit("Success")
