# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.landing

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.profile

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.landingprofile

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.quotation

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.event

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.policy

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_silver.watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO wtdw_silver.watermark as t 
# MAGIC USING (
# MAGIC select 1 as watermarkid, 'wtdw_silver.landing' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 2 as watermarkid, 'wtdw_silver.profile' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 3 as watermarkid, 'wtdw_silver.landingprofile' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 4 as watermarkid, 'wtdw_silver.quotation' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 5 as watermarkid, 'wtdw_silver.event' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 6 as watermarkid, 'wtdw_silver.policy' as TableName, '2024-07-01' as WatermarkValue
# MAGIC )AS s 
# MAGIC ON t.watermarkid = s.watermarkid
# MAGIC WHEN NOT MATCHED THEN INSERT(watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.WatermarkValue)

# COMMAND ----------

dbutils.notebook.exit("Success")
