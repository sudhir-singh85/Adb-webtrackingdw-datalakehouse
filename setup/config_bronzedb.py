# Databricks notebook source
# MAGIC %run "../setup/config_webtrackingdw"

# COMMAND ----------

bronze_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS wtdw_bronze
# MAGIC LOCATION 'f"{bronze_folder_path}"'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_bronze.watermark(watermarkid int,
# MAGIC TableName varchar(100),
# MAGIC WatermarkValue Date)Using DELTA
# MAGIC Location 'f"{bronze_folder_path}"/watermark'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_bronze.landing(
# MAGIC id long,
# MAGIC landingsession varchar(36),
# MAGIC ipadd varchar(15),
# MAGIC browser varchar(255),
# MAGIC device varchar(255),
# MAGIC os varchar(255),
# MAGIC Createdon varchar(255),
# MAGIC Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{bronze_folder_path}"/landing'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_bronze.landingprofile
# MAGIC (
# MAGIC id long,
# MAGIC landingid long,
# MAGIC Vehicle varchar(255),
# MAGIC dob varchar(50),
# MAGIC email varchar(255),
# MAGIC gender varchar(10),
# MAGIC profession varchar(255),
# MAGIC district varchar(50),
# MAGIC Createdon varchar(255),
# MAGIC Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{bronze_folder_path}"/landingprofile'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_bronze.quotation
# MAGIC (
# MAGIC Id long,
# MAGIC landingprofileid long,
# MAGIC Org varchar(255),
# MAGIC QuotationType varchar(255),
# MAGIC QuotationRef varchar(255),
# MAGIC Amount numeric(18,2),
# MAGIC Createdon varchar(255),
# MAGIC Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{bronze_folder_path}"/quotation'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_bronze.event
# MAGIC (
# MAGIC Id long,
# MAGIC QuotationID long,
# MAGIC Org varchar(255),
# MAGIC EventType varchar(50),
# MAGIC QuotationRef varchar(50),
# MAGIC EventRef varchar(50),
# MAGIC Createdon varchar(255),
# MAGIC Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{bronze_folder_path}"/event'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_bronze.policyref
# MAGIC (
# MAGIC Id long,
# MAGIC PolicyNo varchar(255),
# MAGIC EventRef varchar(50),
# MAGIC ExpiryDate varchar(50),
# MAGIC Createdon varchar(255),
# MAGIC Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{bronze_folder_path}"/policyref'
