# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_browser

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_devicetype

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_operatingsystem

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_organisation

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_age

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.dim_profession

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.fact_landing

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.fact_dailysummary

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.fact_orgsummary

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE wtdw_gold.fact_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO wtdw_gold.watermark as t 
# MAGIC USING (
# MAGIC select 1 as watermarkid, 'wtdw_gold.fact_landing' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 2 as watermarkid, 'wtdw_gold.fact_dailysummary' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 3 as watermarkid, 'wtdw_gold.fact_orgsummary' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 4 as watermarkid, 'wtdw_gold.fact_profile' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 5 as watermarkid, 'wtdw_gold.dim_browser' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 6 as watermarkid, 'wtdw_gold.dim_devicetype' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 7 as watermarkid, 'wtdw_gold.dim_operatingsystem' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 8 as watermarkid, 'wtdw_gold.dim_organisation' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 9 as watermarkid, 'wtdw_gold.dim_profile' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 10 as watermarkid, 'wtdw_gold.dim_age' as TableName, '2024-07-01' as WatermarkValue
# MAGIC union
# MAGIC select 11 as watermarkid, 'wtdw_gold.dim_profession' as TableName, '2024-07-01' as WatermarkValue
# MAGIC )AS s 
# MAGIC ON t.watermarkid = s.watermarkid
# MAGIC WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.WatermarkValue)

# COMMAND ----------

dbutils.notebook.exit("Success")
