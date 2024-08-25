# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_bronze.landing

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_bronze.landingprofile

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_bronze.quotation

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_bronze.event

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_bronze.policyref

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_bronze.landing

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO wtdw_bronze.watermark as t 
# MAGIC USING (select TableName, try_cast('2024-07-01' as Date)as MaxDate
# MAGIC from wtdw_bronze.watermark)AS s 
# MAGIC ON t.TableName = s.TableName
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate

# COMMAND ----------

dbutils.notebook.exit("Success")
