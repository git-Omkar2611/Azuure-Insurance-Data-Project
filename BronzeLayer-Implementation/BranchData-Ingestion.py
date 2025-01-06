# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = "branch_id int , \
branch_country  string , \
branch_city string "


branchData_df = spark.read.parquet('/mnt/landing/BranchData/*.parquet' ,schema = schema , header = True , inferSchema = False)

# COMMAND ----------

branchData_df_merged_flag = branchData_df.withColumn("merge_flag" , lit(False))

branchData_df_merged_flag.write.option('path','/mnt/bronzelayer/Branch').mode('append').saveAsTable('bronzelayer.Branch')


# COMMAND ----------

from datetime import datetime

def filePath(folderPath):
    dbutils.fs.mv("/mnt/landing/BranchData/" , "/mnt/processed/BranchData/{}/".format(folderPath) , True)


folderPath = datetime.now().strftime('%m-%d-%Y')

filePath(folderPath)


