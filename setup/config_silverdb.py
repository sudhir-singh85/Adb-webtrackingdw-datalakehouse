# Databricks notebook source
# MAGIC %run "../setup/config_webtrackingdw"

# COMMAND ----------

silver_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS wtdw_silver
# MAGIC LOCATION 'f"{silver_folder_path}"'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.watermark(watermarkid int,
# MAGIC TableName varchar(100),
# MAGIC WatermarkValue Date)Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/watermark'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.landing(
# MAGIC landingid INT,
# MAGIC landingsession varchar(36),
# MAGIC ipaddress varchar(50),
# MAGIC browsername varchar(50),
# MAGIC devicename varchar(50),
# MAGIC operatingsystem varchar(50),
# MAGIC createdon TIMESTAMP,
# MAGIC ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/landing'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.profile(
# MAGIC profileid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC vehicledata varchar(255),
# MAGIC dob Date,
# MAGIC emailaddress varchar(255),
# MAGIC gender varchar(50),
# MAGIC resi_county varchar(50),
# MAGIC ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/profile'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.landingprofile(
# MAGIC landingprofileid INT,
# MAGIC landingid INT,
# MAGIC profileid INT,
# MAGIC professionname varchar(50),
# MAGIC isunique BOOLEAN,
# MAGIC createdon TIMESTAMP,
# MAGIC ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/landingprofile'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.quotation(
# MAGIC quotationid INT,
# MAGIC landingprofileid INT,
# MAGIC quotationtype varchar(50),
# MAGIC refno varchar(50),
# MAGIC org_name varchar(50),
# MAGIC quotation_amt FLOAT,
# MAGIC createdon TIMESTAMP,
# MAGIC ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/quotation'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.event(
# MAGIC eventid INT,
# MAGIC quotationid INT,
# MAGIC profileid INT,
# MAGIC org_name varchar(50),
# MAGIC eventtype varchar(50),
# MAGIC event_ref varchar(50),
# MAGIC quotation_ref varchar(50),
# MAGIC isunique BOOLEAN,
# MAGIC createdon TIMESTAMP,
# MAGIC ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/event'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_silver.policy(
# MAGIC policyid INT,
# MAGIC policyno varchar(50),
# MAGIC event_ref varchar(50),
# MAGIC createdon TIMESTAMP,
# MAGIC expirydate TIMESTAMP,
# MAGIC ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{silver_folder_path}"/policy'

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
