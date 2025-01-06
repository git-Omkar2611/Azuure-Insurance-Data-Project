# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = "claim_id int , \
policy_id  int , \
date_of_claim timestamp ,\
claim_amount double ,\
claim_status  string ,\
LastUpdatedTimeStamp  timestamp "


claimData_df = spark.read.parquet('/mnt/landing/ClaimData/*.parquet' ,schema = schema , inferSchema = False)

# COMMAND ----------

claimData_df_with_merged_Flag = claimData_df.withColumn("merge_flag" , lit(False))
#claimData_df_with_merged_Flag.show()

claimData_df_with_merged_Flag.write.option('path', '/mnt/bronzelayer/Claim').mode("append").saveAsTable("bronzelayer.claim")

# COMMAND ----------

from datetime import datetime

def filePath(folderPath):
    dbutils.fs.mv("/mnt/landing/ClaimData/" , "/mnt/processed/ClaimData/{}/".format(folderPath) , True)


folderPath = datetime.now().strftime('%m-%d-%Y')

filePath(folderPath)