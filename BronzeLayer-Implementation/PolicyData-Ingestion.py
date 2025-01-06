# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = "policy_id int , \
policy_type  string , \
customer_id int , \
start_date timestamp , \
end_date timestamp , \
premium double , \
coverage_amount double"


policyData_df = spark.read.json('/mnt/landing/PolicyData/*.json' ,schema = schema)

policyData_df.show()

# COMMAND ----------

policyData_df_merged_Flag = policyData_df.withColumn("merged", lit(False))

policyData_df_merged_Flag.write.option('path' , '/mnt/bronzelayer/Policy').mode('append').saveAsTable('bronzelayer.Policy')

# COMMAND ----------

from datetime import datetime

def filePath(folderPath):
    dbutils.fs.mv("/mnt/landing/PolicyData/" , "/mnt/processed/PolicyData/{}/".format(folderPath) , True)

folderPath = datetime.now().strftime('%m-%d-%Y')

filePath(folderPath)


