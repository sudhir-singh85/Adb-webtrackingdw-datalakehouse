# Databricks notebook source
# MAGIC %run "../setup/config_webtrackingdw"

# COMMAND ----------

gold_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS wtdw_gold
# MAGIC LOCATION 'f"{gold_folder_path}"'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.watermark(watermarkid int,
# MAGIC TableName varchar(100),
# MAGIC WatermarkValue Date)Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/watermark'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.datedim(
# MAGIC datekey int,
# MAGIC d_date Date,
# MAGIC d_year INT,
# MAGIC d_month INT,
# MAGIC d_day int
# MAGIC )
# MAGIC Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/datedim'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_browser(
# MAGIC browserid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC browsername varchar(50),
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_browser'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_devicetype(
# MAGIC devicetypeid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC devicename varchar(50),
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_devicetype'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_operatingsystem(
# MAGIC operatingsystemid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC osname varchar(50),
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_operatingsystem'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_organisation(
# MAGIC organisationid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC org_name varchar(50),
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_organisation'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_profile(
# MAGIC profileid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC vehicledata varchar(255),
# MAGIC dob DATE,
# MAGIC emailaddress VARCHAR(255),
# MAGIC gender varchar(50),
# MAGIC resi_county varchar(50),
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_profile'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_age(
# MAGIC ageid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC age_group varchar(50),
# MAGIC age INT,
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_age'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.dim_profession(
# MAGIC professionid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC professionname varchar(50),
# MAGIC isactive BOOLEAN,
# MAGIC createdon TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/dim_profession'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.fact_landing(
# MAGIC fact_landingid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC datekey INT,
# MAGIC browserid INT,
# MAGIC devicetypeid INT,
# MAGIC operatingsystemid INT,
# MAGIC landing_count INT,
# MAGIC uniquelanding_count INT,
# MAGIC ingestion_date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/fact_landing'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.fact_dailysummary(
# MAGIC fact_dailysummaryid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC datekey INT,
# MAGIC request_count INT,
# MAGIC requestwithresponse_count INT,
# MAGIC requestwithoutresponse_count INT,
# MAGIC uniquerequest_count INT,
# MAGIC uniquerequestwithresponse_count INT,
# MAGIC uniquerequestwithoutresponse_count INT,
# MAGIC buy_event INT,
# MAGIC agent_event INT,
# MAGIC call_event INT,
# MAGIC uniquebuy_event INT,
# MAGIC uniqueagent_event INT,
# MAGIC uniquecall_event INT,
# MAGIC buy_sale INT,
# MAGIC agent_sale INT,
# MAGIC call_sale INT,
# MAGIC ingestion_date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/fact_dailysummary'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.fact_orgsummary(
# MAGIC fact_orgsummaryid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC datekey INT,
# MAGIC organisationid INT,
# MAGIC request_count INT,
# MAGIC tp_responsecount INT,
# MAGIC tp_avgquotation FLOAT,
# MAGIC tpe_responsecount INT,
# MAGIC tpe_avgquotation FLOAT,
# MAGIC c_responsecount INT,
# MAGIC c_avgquotation FLOAT,
# MAGIC buy_event INT,
# MAGIC agent_event INT,
# MAGIC call_event INT,
# MAGIC buy_sale INT,
# MAGIC agent_sale INT,
# MAGIC call_sale INT,
# MAGIC ingestion_date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/fact_orgsummary'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS wtdw_gold.fact_profile(
# MAGIC fact_profileid BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC datekey INT,
# MAGIC profileid INT,
# MAGIC ageid INT,
# MAGIC professionid INT,
# MAGIC devicetypeid INT,
# MAGIC operatingsystemid INT,
# MAGIC requestwithresponse_count INT,
# MAGIC requestwithoutresponse_count INT,
# MAGIC tp_responsecount INT,
# MAGIC tpe_responsecount INT,
# MAGIC c_responsecount INT,
# MAGIC buy_event INT,
# MAGIC agent_event INT,
# MAGIC call_event INT,
# MAGIC buy_sale INT,
# MAGIC agent_sale INT,
# MAGIC call_sale INT,
# MAGIC ingestion_date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location 'f"{gold_folder_path}"/fact_profile'

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
