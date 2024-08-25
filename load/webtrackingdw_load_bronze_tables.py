# Databricks notebook source
# MAGIC %run "../setup/config_webtrackingdw"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dataset_schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("landingsession", StringType(), nullable=True),
    StructField("ipadd", StringType(), nullable=True),
    StructField("browser", StringType(), nullable=True),
    StructField("device", StringType(), nullable=True),
    StructField("os", StringType(), nullable=True),
    StructField("Createdon", TimestampType(), nullable=True)])

# COMMAND ----------

read_df = spark.read.option("header",True)\
    .schema(dataset_schema)\
    .csv(f"{raw_folder_path}/landing/*.csv")

# COMMAND ----------

df2 = read_df.withColumn("Ingestion_Date", F.current_timestamp())

# COMMAND ----------

df2.createOrReplaceTempView ("v_landing")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_bronze.landing(id,landingsession,ipadd,browser,device,os,Createdon,Ingestion_Date)
# MAGIC Select id,landingsession,ipadd,browser,device,os,Createdon,Ingestion_Date
# MAGIC from v_landing
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_bronze.watermark where TableName = 'wtdw_bronze.landing')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_bronze.watermark
# MAGIC set WatermarkValue = (Select max(Createdon) from wtdw_bronze.landing)
# MAGIC where TableName = 'wtdw_bronze.landing'

# COMMAND ----------

dataset_schema_landingprofile = StructType([
    StructField("id", StringType(), nullable=True),
	StructField("landingid", StringType(), nullable=True),
 StructField("Vehicle", StringType(), nullable=True),
	StructField("dob", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("profession", StringType(), nullable=True),
    StructField("district", StringType(), nullable=True),
    StructField("Createdon", StringType(), nullable=True)])

# COMMAND ----------

read_df_landingprofile = spark.read.option("header",True)\
    .option("delimiter", "\t")\
    .schema(dataset_schema_landingprofile)\
    .csv(f"{raw_folder_path}/landingprofile/*.txt")	\
    .withColumn("Ingestion_Date", F.current_timestamp())

# COMMAND ----------

read_df_landingprofile.createOrReplaceTempView ("v_landingprofile")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_bronze.landingprofile(id,landingid,Vehicle,dob,email,gender,profession,district,Createdon,Ingestion_Date)
# MAGIC Select id,landingid,Vehicle,dob,email,gender,profession,district,Createdon,Ingestion_Date
# MAGIC from v_landingprofile
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_bronze.watermark where TableName = 'wtdw_bronze.landingprofile')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_bronze.watermark
# MAGIC set WatermarkValue = (Select max(Createdon) from wtdw_bronze.landingprofile)
# MAGIC where TableName = 'wtdw_bronze.landingprofile'

# COMMAND ----------

dataset_schema_quotation = StructType([
    StructField("Id", IntegerType(), nullable=True),
	StructField("landingprofileid", IntegerType(), nullable=True),
	StructField("Org", StringType(), nullable=True),
    StructField("QuotationType", StringType(), nullable=True),
    StructField("QuotationRef", StringType(), nullable=True),
    StructField("Amount", StringType(), nullable=True),
    StructField("CreatedOn", StringType(), nullable=True)])

# COMMAND ----------

read_df_quotation = spark.read.option("header",True)\
    .schema(dataset_schema_quotation)\
        .option("multiline", "true")\
            .json(f"{raw_folder_path}/quotation/*.json")\
             .withColumn("Ingestion_Date", F.current_timestamp())

# COMMAND ----------

read_df_quotation.createOrReplaceTempView ("v_quotation")	

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_bronze.quotation(Id,landingprofileid,Org,QuotationType,QuotationRef,Amount,Createdon,Ingestion_Date)
# MAGIC Select Id,landingprofileid,Org,QuotationType,QuotationRef,Amount,Createdon,Ingestion_Date
# MAGIC from v_quotation
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_bronze.watermark where TableName = 'wtdw_bronze.quotation')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_bronze.watermark
# MAGIC set WatermarkValue = (Select max(Createdon) from wtdw_bronze.quotation)
# MAGIC where TableName = 'wtdw_bronze.quotation'

# COMMAND ----------

dataset_schema_event = StructType([
    StructField("Id", IntegerType(), nullable=True),
	StructField("QuotationID", IntegerType(), nullable=True),
	StructField("Org", StringType(), nullable=True),
    StructField("EventType", StringType(), nullable=True),
    StructField("QuotationRef", StringType(), nullable=True),
    StructField("EventRef", StringType(), nullable=True),
    StructField("CreatedOn", StringType(), nullable=True)])

# COMMAND ----------

read_df_event = spark.read.option("header",True)\
    .schema(dataset_schema_event)\
    .csv(f"{raw_folder_path}/event/*.csv")\
        .withColumn("Ingestion_Date", F.current_timestamp())

# COMMAND ----------

read_df_event.createOrReplaceTempView ("v_event")	

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_bronze.event(Id,QuotationID,Org,EventType,QuotationRef,EventRef,Createdon,Ingestion_Date)
# MAGIC Select Id,QuotationID,Org,EventType,QuotationRef,EventRef,Createdon,Ingestion_Date
# MAGIC from v_event
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_bronze.watermark where TableName = 'wtdw_bronze.event')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_bronze.watermark
# MAGIC set WatermarkValue = (Select max(Createdon) from wtdw_bronze.event)
# MAGIC where TableName = 'wtdw_bronze.event'

# COMMAND ----------

dataset_schema_policyref = StructType([
    StructField("id", StringType(), nullable=True),
	StructField("PolicyNo", StringType(), nullable=True),
 StructField("EventRef", StringType(), nullable=True),
	StructField("ExpiryDate", StringType(), nullable=True),
    StructField("Createdon", StringType(), nullable=True)])

# COMMAND ----------

read_df_policyref = spark.read.option("header",True)\
    .option("delimiter", "\t")\
    .schema(dataset_schema_policyref)\
    .csv(f"{raw_folder_path}/policyref/*.txt")\
        .withColumn("Ingestion_Date", F.current_timestamp())

# COMMAND ----------

read_df_policyref.createOrReplaceTempView ("v_policyref")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into wtdw_bronze.policyref(id,PolicyNo,EventRef,ExpiryDate,Createdon,Ingestion_Date)
# MAGIC Select id,PolicyNo,EventRef,ExpiryDate,Createdon,Ingestion_Date
# MAGIC from v_policyref
# MAGIC WHERE try_cast(Createdon as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from wtdw_bronze.watermark where TableName = 'wtdw_bronze.policyref')

# COMMAND ----------

# MAGIC %sql
# MAGIC update wtdw_bronze.watermark
# MAGIC set WatermarkValue = (Select max(Createdon) from wtdw_bronze.policyref)
# MAGIC where TableName = 'wtdw_bronze.policyref'
