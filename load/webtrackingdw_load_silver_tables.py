# Databricks notebook source
# MAGIC %sql
# MAGIC insert into wtdw_silver.landing(landingid,landingsession,ipaddress,browsername,devicename,operatingsystem,createdon,ingestion_Date)
# MAGIC Select id as landingid,landingsession,ipadd as ipaddress,browser as browsername,device as devicename,os as operatingsystem,cast(Createdon as timestamp) as createdon,getdate() as ingestion_Date
# MAGIC from  wtdw_bronze.landing
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landing')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_silver.watermark
# MAGIC set WatermarkValue = (Select max(createdon) from wtdw_silver.landing)
# MAGIC where TableName = 'wtdw_silver.landing'

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into wtdw_silver.profile as target
# MAGIC using(select Vehicle as vehicledata,try_cast(dob as Date) as dob,email as emailaddress,gender,district as resi_county
# MAGIC from wtdw_bronze.landingprofile
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landingprofile')
# MAGIC ) as Source
# MAGIC On target.vehicledata = Source.vehicledata and target.dob = Source.dob and target.emailaddress = Source.emailaddress and target.gender = Source.gender
# MAGIC  and target.resi_county = Source.resi_county
# MAGIC  
# MAGIC When not Matched then Insert(vehicledata,dob,emailaddress,gender,resi_county)
# MAGIC Values(Source.vehicledata, Source.dob, Source.emailaddress, Source.gender, Source.resi_county)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_silver.landingprofile(landingprofileid,landingid,profileid,professionname,isunique,createdon,ingestion_Date)
# MAGIC Select id as landingprofileid,landingid,p.profileid,lp.profession as professionname,0 as isunique,cast(Createdon as timestamp) as createdon,getdate() as ingestion_Date
# MAGIC from  wtdw_bronze.landingprofile as lp
# MAGIC Inner Join wtdw_silver.profile as p on lp.Vehicle = p.vehicledata and lp.dob = p.dob and lp.email = p.emailaddress and lp.gender = p.gender and lp.district = p.resi_county
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landingprofile')

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT 
# MAGIC     l.landingprofileid,
# MAGIC     CASE 
# MAGIC       WHEN l1.landingprofileid IS NULL THEN 1 
# MAGIC       ELSE 0 
# MAGIC     END AS new_isunique
# MAGIC   FROM wtdw_silver.landingprofile l
# MAGIC   LEFT JOIN wtdw_silver.landingprofile l1 
# MAGIC     ON l.profileid = l1.profileid 
# MAGIC     AND l1.createdon > dateadd(day, -30, l.createdon)
# MAGIC     and l1.landingprofileid<l.landingprofileid
# MAGIC     WHERE l.createdon > (
# MAGIC     SELECT COALESCE(WatermarkValue, '2024-01-01') 
# MAGIC     FROM wtdw_silver.watermark 
# MAGIC     WHERE TableName = 'wtdw_silver.landingprofile'
# MAGIC   )
# MAGIC )
# MAGIC MERGE INTO wtdw_silver.landingprofile
# MAGIC USING CTE
# MAGIC ON wtdw_silver.landingprofile.landingprofileid = CTE.landingprofileid
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET isunique = CTE.new_isunique;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_silver.watermark
# MAGIC set WatermarkValue = (Select max(createdon) from wtdw_silver.landingprofile)
# MAGIC where TableName = 'wtdw_silver.landingprofile'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_silver.quotation(quotationid,landingprofileid,quotationtype,refno,org_name,quotation_amt,createdon,ingestion_Date)
# MAGIC Select Id as quotationid,landingprofileid as landingprofileid,QuotationType as quotationtype,QuotationRef as refno,Org as org_name,Amount as quotation_amt,cast(Createdon as timestamp) as createdon,getdate() as ingestion_Date
# MAGIC from  wtdw_bronze.quotation
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.quotation')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_silver.watermark
# MAGIC set WatermarkValue = (Select max(createdon) from wtdw_silver.quotation)
# MAGIC where TableName = 'wtdw_silver.quotation'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_silver.event(eventid,quotationid,profileid,org_name,eventtype,event_ref,quotation_ref,isunique,createdon,ingestion_Date)
# MAGIC Select Id as eventid,e.QuotationID as quotationid,lp.profileid,Org as org_name,EventType as eventtype,EventRef as event_ref,QuotationRef as quotation_ref,0 as isunique,cast(e.Createdon as timestamp) as createdon,getdate() as ingestion_Date
# MAGIC from  wtdw_bronze.event e
# MAGIC inner join wtdw_silver.quotation q on e.QuotationID = q.quotationid
# MAGIC inner join wtdw_silver.landingprofile lp on q.landingprofileid = lp.landingprofileid
# MAGIC WHERE try_cast(e.Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.event')

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT DISTINCT
# MAGIC     l.eventid,
# MAGIC     CASE 
# MAGIC       WHEN l1.eventid IS NULL THEN TRUE 
# MAGIC       ELSE FALSE 
# MAGIC     END AS new_isunique
# MAGIC   FROM wtdw_silver.event l
# MAGIC   LEFT JOIN wtdw_silver.event l1 
# MAGIC     ON l.profileid = l1.profileid 
# MAGIC     AND l.org_name = l1.org_name 
# MAGIC     AND l.eventtype = l1.eventtype 
# MAGIC     AND l1.createdon > dateadd(day, -30, l.createdon)
# MAGIC     and l1.eventid <l.eventid
# MAGIC   WHERE l.createdon > (
# MAGIC     SELECT COALESCE(MAX(WatermarkValue), '2024-01-01') 
# MAGIC     FROM wtdw_silver.watermark 
# MAGIC     WHERE TableName = 'wtdw_silver.event'
# MAGIC   )
# MAGIC )
# MAGIC MERGE INTO wtdw_silver.event
# MAGIC USING CTE
# MAGIC ON wtdw_silver.event.eventid = CTE.eventid
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET isunique = CTE.new_isunique;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_silver.watermark
# MAGIC set WatermarkValue = (Select max(createdon) from wtdw_silver.event)
# MAGIC where TableName = 'wtdw_silver.event'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_silver.policy(policyid,policyno,event_ref,createdon,expirydate,ingestion_Date)
# MAGIC Select Id as policyid,PolicyNo as policyno,EventRef as event_ref,cast(ExpiryDate as timestamp) as expirydate,cast(Createdon as timestamp) as createdon,getdate() as ingestion_Date
# MAGIC from  wtdw_bronze.policyref
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.policy')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_silver.watermark
# MAGIC set WatermarkValue = (Select max(createdon) from wtdw_silver.policy)
# MAGIC where TableName = 'wtdw_silver.policy'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * From wtdw_silver.event
