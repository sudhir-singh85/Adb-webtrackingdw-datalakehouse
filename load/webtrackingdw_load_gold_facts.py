# Databricks notebook source
# DBTITLE 1,populate fact_landing
# MAGIC %sql
# MAGIC insert into wtdw_gold.fact_landing(datekey,browserid,devicetypeid,operatingsystemid,landing_count,uniquelanding_count,ingestion_Date)
# MAGIC Select datekey,browserid,devicetypeid,operatingsystemid,count(l.landingid) as landing_count,
# MAGIC count(distinct l.landingsession) as uniquelanding_count, getdate() as ingestion_Date
# MAGIC from  wtdw_silver.landing l
# MAGIC left join wtdw_gold.dim_browser br on l.browsername = br.browsername
# MAGIC left join wtdw_gold.dim_devicetype dt on l.devicename = dt.devicename
# MAGIC left join wtdw_gold.dim_operatingsystem os on l.operatingsystem = os.osname
# MAGIC left join wtdw_gold.datedim d on try_cast(l.createdon as Date) = d.d_date
# MAGIC WHERE l.createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.fact_landing')
# MAGIC group by datekey,browserid,devicetypeid,operatingsystemid

# COMMAND ----------

# DBTITLE 1,update watermark for fact_landing
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select max(d_date) from wtdw_gold.fact_landing l 
# MAGIC                       inner join wtdw_gold.datedim d on l.datekey = d.datekey)
# MAGIC where TableName = 'wtdw_gold.fact_landing'

# COMMAND ----------

# DBTITLE 1,populate fact_dailysummary
# MAGIC %sql
# MAGIC insert into wtdw_gold.fact_dailysummary(datekey,request_count,requestwithresponse_count,requestwithoutresponse_count,
# MAGIC uniquerequest_count,uniquerequestwithresponse_count,uniquerequestwithoutresponse_count,buy_event,
# MAGIC agent_event,call_event,uniquebuy_event,uniqueagent_event,uniquecall_event,buy_sale,agent_sale,call_sale,ingestion_date)
# MAGIC Select d.datekey
# MAGIC ,count(distinct lp.landingprofileid) as request_count
# MAGIC ,count(distinct case when q.landingprofileid is not null then q.landingprofileid else null end) as requestwithresponse_count
# MAGIC ,count(distinct case when q.landingprofileid is null then lp.landingprofileid else null end) as requestwithoutresponse_count
# MAGIC ,count(distinct case when lp.isunique = 1 then lp.landingprofileid else null end) as uniquerequest_count
# MAGIC ,count(distinct case when q.landingprofileid is not null and lp.isunique = 1 then q.landingprofileid else null end) as uniquerequestwithresponse_count
# MAGIC ,count(distinct case when q.landingprofileid is null and lp.isunique = 1 then lp.landingprofileid else null end) as uniquerequestwithoutresponse_count
# MAGIC ,count(distinct case when e.eventtype = 'buy' then e.eventid else null end) as buy_event
# MAGIC ,count(distinct case when e.eventtype = 'Agent' then e.eventid else null end) as agent_event
# MAGIC ,count(distinct case when e.eventtype = 'Call' then e.eventid else null end) as call_event
# MAGIC ,count(distinct case when e.eventtype = 'buy' and e.isunique = 1 then e.eventid else null end) as uniquebuy_event
# MAGIC ,count(distinct case when e.eventtype = 'Agent' and e.isunique = 1 then e.eventid else null end) as uniqueagent_event
# MAGIC ,count(distinct case when e.eventtype = 'Call' and e.isunique = 1 then e.eventid else null end) as uniquecall_event
# MAGIC ,count(distinct case when e.eventtype = 'buy' then p.policyid else null end) as buy_sale
# MAGIC ,count(distinct case when e.eventtype = 'Agent' then p.policyid else null end) as agent_sale
# MAGIC ,count(distinct case when e.eventtype = 'Call' then p.policyid else null end) as call_sale
# MAGIC , getdate() as ingestion_Date
# MAGIC from  wtdw_silver.landingprofile lp
# MAGIC inner join wtdw_gold.datedim d on try_cast(lp.createdon as Date) = d.d_date
# MAGIC left join wtdw_silver.quotation q on lp.landingprofileid = q.landingprofileid
# MAGIC left join wtdw_silver.event e on q.quotationid = e.quotationid
# MAGIC left join wtdw_silver.policy p on e.event_ref = p.event_ref
# MAGIC WHERE lp.createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.fact_dailysummary')
# MAGIC group by datekey

# COMMAND ----------

# DBTITLE 1,update watermark for fact_dailysummary
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select max(d_date) from wtdw_gold.fact_dailysummary l 
# MAGIC                       inner join wtdw_gold.datedim d on l.datekey = d.datekey)
# MAGIC where TableName = 'wtdw_gold.fact_dailysummary'

# COMMAND ----------

# DBTITLE 1,populate fact_orgsummary
# MAGIC %sql
# MAGIC insert into wtdw_gold.fact_orgsummary(datekey,organisationid,request_count,tp_responsecount,tp_avgquotation
# MAGIC ,tpe_responsecount,tpe_avgquotation,c_responsecount,c_avgquotation,buy_event,agent_event,call_event,buy_sale,agent_sale,call_sale,ingestion_date)
# MAGIC Select d.datekey
# MAGIC ,o.organisationid
# MAGIC ,count(distinct qp.landingprofileid) as request_count
# MAGIC ,count(distinct case when qp.quotationtype= 'TP' then qp.landingprofileid else null end) as tp_responsecount
# MAGIC ,avg(case when qp.quotationtype= 'TP' then qp.quotation_amt else null end) as tp_avgquotation
# MAGIC ,count(distinct case when qp.quotationtype= 'TPE' then qp.landingprofileid else null end) as tpe_responsecount
# MAGIC ,avg(case when qp.quotationtype= 'TPE' then qp.quotation_amt else null end) as tpe_avgquotation
# MAGIC ,count(distinct case when qp.quotationtype= 'C' then qp.landingprofileid else null end) as c_responsecount
# MAGIC ,avg(case when qp.quotationtype= 'C' then qp.quotation_amt else null end) as c_avgquotation
# MAGIC ,count(distinct case when e.eventtype = 'buy' then e.eventid else null end) as buy_event
# MAGIC ,count(distinct case when e.eventtype = 'Agent' then e.eventid else null end) as agent_event
# MAGIC ,count(distinct case when e.eventtype = 'Call' then e.eventid else null end) as call_event
# MAGIC ,count(distinct case when e.eventtype = 'buy' then p.policyid else null end) as buy_sale
# MAGIC ,count(distinct case when e.eventtype = 'Agent' then p.policyid else null end) as agent_sale
# MAGIC ,count(distinct case when e.eventtype = 'Call' then p.policyid else null end) as call_sale
# MAGIC , getdate() as ingestion_Date
# MAGIC from  wtdw_silver.quotation qp
# MAGIC inner join wtdw_gold.datedim d on try_cast(qp.createdon as Date) = d.d_date
# MAGIC inner join wtdw_gold.dim_organisation o on qp.org_name = o.org_name
# MAGIC left join wtdw_silver.event e on qp.quotationid = e.quotationid
# MAGIC left join wtdw_silver.policy p on e.event_ref = p.event_ref
# MAGIC WHERE qp.createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.fact_orgsummary')
# MAGIC group by datekey,organisationid

# COMMAND ----------

# DBTITLE 1,update watermark for fact_orgsummary
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select max(d_date) from wtdw_gold.fact_orgsummary l 
# MAGIC                       inner join wtdw_gold.datedim d on l.datekey = d.datekey)
# MAGIC where TableName = 'wtdw_gold.fact_orgsummary'

# COMMAND ----------

# DBTITLE 1,populate fact_profile
# MAGIC %sql
# MAGIC insert into wtdw_gold.fact_profile(datekey,profileid,ageid,professionid,devicetypeid,operatingsystemid,requestwithresponse_count,requestwithoutresponse_count,tp_responsecount,tpe_responsecount,c_responsecount,buy_event,agent_event,call_event,buy_sale,agent_sale,call_sale,ingestion_date)
# MAGIC Select d.datekey
# MAGIC ,lp.profileid
# MAGIC ,age.ageid
# MAGIC ,professionid
# MAGIC ,devicetypeid
# MAGIC ,operatingsystemid
# MAGIC ,count(distinct case when qp.landingprofileid is not null then qp.landingprofileid else null end) as requestwithresponse_count
# MAGIC ,count(distinct case when qp.landingprofileid is null then lp.landingprofileid else null end) as requestwithoutresponse_count
# MAGIC ,count(distinct case when qp.quotationtype= 'TP' then qp.landingprofileid else null end) as tp_responsecount
# MAGIC ,count(distinct case when qp.quotationtype= 'TPE' then qp.landingprofileid else null end) as tpe_responsecount
# MAGIC ,count(distinct case when qp.quotationtype= 'C' then qp.landingprofileid else null end) as c_responsecount
# MAGIC ,count(distinct case when e.eventtype = 'buy' then e.eventid else null end) as buy_event
# MAGIC ,count(distinct case when e.eventtype = 'Agent' then e.eventid else null end) as agent_event
# MAGIC ,count(distinct case when e.eventtype = 'Call' then e.eventid else null end) as call_event
# MAGIC ,count(distinct case when e.eventtype = 'buy' then p.policyid else null end) as buy_sale
# MAGIC ,count(distinct case when e.eventtype = 'Agent' then p.policyid else null end) as agent_sale
# MAGIC ,count(distinct case when e.eventtype = 'Call' then p.policyid else null end) as call_sale
# MAGIC , getdate() as ingestion_Date
# MAGIC from  wtdw_silver.landingprofile lp
# MAGIC inner join wtdw_silver.landing l on lp.landingid = l.landingid
# MAGIC inner join wtdw_silver.profile pro on lp.profileid = pro.profileid
# MAGIC inner join wtdw_gold.datedim d on try_cast(lp.createdon as Date) = d.d_date
# MAGIC inner join wtdw_silver.quotation qp on lp.landingprofileid = qp.landingprofileid
# MAGIC left join wtdw_gold.dim_age age on  datediff(Year,dob,lp.createdon) = age.age
# MAGIC left join wtdw_gold.dim_devicetype dt on l.devicename = dt.devicename
# MAGIC left join wtdw_gold.dim_operatingsystem os on l.operatingsystem = os.osname
# MAGIC left join wtdw_gold.dim_profession prof on lp.professionname = prof.professionname
# MAGIC left join wtdw_silver.event e on qp.quotationid = e.quotationid
# MAGIC left join wtdw_silver.policy p on e.event_ref = p.event_ref
# MAGIC WHERE lp.createdon>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_gold.watermark where TableName = 'wtdw_gold.fact_profile')
# MAGIC group by d.datekey,lp.profileid,age.ageid,professionid,devicetypeid,operatingsystemid

# COMMAND ----------

# DBTITLE 1,update watermark for fact_profile
# MAGIC %sql
# MAGIC update wtdw_gold.watermark
# MAGIC set WatermarkValue = (Select max(d_date) from wtdw_gold.fact_profile l 
# MAGIC                       inner join wtdw_gold.datedim d on l.datekey = d.datekey)
# MAGIC where TableName = 'wtdw_gold.fact_profile'
