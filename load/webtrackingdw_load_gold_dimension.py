# Databricks notebook source
# DBTITLE 1,populate dim_browser dimension
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_browser as target
# MAGIC using(select distinct browsername, 1 as isactive
# MAGIC from wtdw_silver.landing
# MAGIC WHERE createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_browser')
# MAGIC ) as Source
# MAGIC On target.browsername = Source.browsername
# MAGIC  
# MAGIC When not Matched then Insert(browsername,isactive,createdon)
# MAGIC Values(Source.browsername, Source.isactive, getdate())

# COMMAND ----------

# DBTITLE 1,update watermark for dim_browser
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landing')
# MAGIC where TableName = 'wtdw_gold.dim_browser'

# COMMAND ----------

# DBTITLE 1,populate dim_devicetype dimension
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_devicetype as target
# MAGIC using(select distinct devicename, 1 as isactive
# MAGIC from wtdw_silver.landing
# MAGIC WHERE createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_devicetype')
# MAGIC ) as Source
# MAGIC On target.devicename = Source.devicename
# MAGIC  
# MAGIC When not Matched then Insert(devicename,isactive,createdon)
# MAGIC Values(Source.devicename, Source.isactive, getdate())

# COMMAND ----------

# DBTITLE 1,update dim_devicetype dimension
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landing')
# MAGIC where TableName = 'wtdw_gold.dim_devicetype'

# COMMAND ----------

# DBTITLE 1,populate dim_operatingsystem dimension
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_operatingsystem as target
# MAGIC using(select distinct operatingsystem, 1 as isactive
# MAGIC from wtdw_silver.landing
# MAGIC WHERE createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_operatingsystem')
# MAGIC ) as Source
# MAGIC On target.osname = Source.operatingsystem
# MAGIC  
# MAGIC When not Matched then Insert(osname,isactive,createdon)
# MAGIC Values(Source.operatingsystem, Source.isactive, getdate())

# COMMAND ----------

# DBTITLE 1,update watermark for dim_operatingsystem
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landing')
# MAGIC where TableName = 'wtdw_gold.dim_operatingsystem'

# COMMAND ----------

# DBTITLE 1,populate dim_organisation dimension
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_organisation as target
# MAGIC using(select distinct org_name, 1 as isactive
# MAGIC from wtdw_silver.quotation
# MAGIC WHERE createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_organisation')
# MAGIC ) as Source
# MAGIC On target.org_name = Source.org_name
# MAGIC  
# MAGIC When not Matched then Insert(org_name,isactive,createdon)
# MAGIC Values(Source.org_name, Source.isactive, getdate())

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.quotation')
# MAGIC where TableName = 'wtdw_gold.dim_organisation'

# COMMAND ----------

# DBTITLE 1,populate dim_profession dimension
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_profession as target
# MAGIC using(select distinct professionname, 1 as isactive
# MAGIC from wtdw_silver.landingprofile
# MAGIC WHERE createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_profession')
# MAGIC ) as Source
# MAGIC On target.professionname = Source.professionname
# MAGIC  
# MAGIC When not Matched then Insert(professionname,isactive,createdon)
# MAGIC Values(Source.professionname, Source.isactive, getdate())

# COMMAND ----------

# DBTITLE 1,update watermark for dim_profession
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landingprofile')
# MAGIC where TableName = 'wtdw_gold.dim_profession'

# COMMAND ----------

# DBTITLE 1,populate dim_profile dimension
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_profile as target
# MAGIC using(select distinct vehicledata,dob,emailaddress,gender,resi_county, 1 as isactive
# MAGIC from wtdw_silver.landingprofile lp
# MAGIC Inner join wtdw_silver.profile p on lp.profileid = p.profileid
# MAGIC WHERE lp.createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_profile')
# MAGIC ) as Source
# MAGIC On target.vehicledata = Source.vehicledata and target.dob = Source.dob and target.emailaddress = Source.emailaddress
# MAGIC and target.resi_county = Source.resi_county
# MAGIC  
# MAGIC When not Matched then Insert(vehicledata,dob,emailaddress,gender,resi_county,isactive,createdon)
# MAGIC Values(Source.vehicledata,Source.dob,Source.emailaddress,Source.gender,Source.resi_county,Source.isactive, getdate())

# COMMAND ----------

# DBTITLE 1,update watermark for dim_profile
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landingprofile')
# MAGIC where TableName = 'wtdw_gold.dim_profile'

# COMMAND ----------

# DBTITLE 1,populate dim_age dimensions
# MAGIC %sql
# MAGIC merge into wtdw_gold.dim_age as target
# MAGIC using(select distinct datediff(Year,dob,lp.createdon) as age,
# MAGIC case when datediff(Year,dob,lp.createdon) <18 then 'Under 18'
# MAGIC when datediff(Year,dob,lp.createdon) between 18 and 35 then '18-35'
# MAGIC when datediff(Year,dob,lp.createdon) between 36 and 55 then '36-55'
# MAGIC when datediff(Year,dob,lp.createdon) > 55 then '55+' end as age_group
# MAGIC ,1 as isactive
# MAGIC from wtdw_silver.landingprofile lp
# MAGIC Inner join wtdw_silver.profile p on lp.profileid = p.profileid
# MAGIC WHERE lp.createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.dim_profile')
# MAGIC ) as Source
# MAGIC On target.age = Source.age and target.age_group = Source.age_group
# MAGIC  
# MAGIC When not Matched then Insert(age_group,age,isactive,createdon)
# MAGIC Values(Source.age_group,Source.age,Source.isactive, getdate())

# COMMAND ----------

# DBTITLE 1,update watermark for dim_age
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_silver.watermark where TableName = 'wtdw_silver.landingprofile')
# MAGIC where TableName = 'wtdw_gold.dim_age'
