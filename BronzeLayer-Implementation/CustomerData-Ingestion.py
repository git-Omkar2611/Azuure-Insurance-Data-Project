# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = "customer_id int , \
    first_name string , \
    last_name string , \
    email string, \
    phone  string, \
    country string , \
    city string, \
    registration_date timestamp , \
    date_of_birth timestamp , \
    gender string"

customerData_df = spark.read.csv('/mnt/landing/Customer' ,schema = schema , header = True , inferSchema = False )

customerData_df.show()

# COMMAND ----------

customerData_df_merged_flag = customerData_df.withColumn("merge_flag", lit(False))

customerData_df_merged_flag.write.option('path','/mnt/bronzelayer/CustomerData').mode('append').saveAsTable('bronzelayer.Customer')

#display(customerData_df_merged_flag)

# COMMAND ----------

from datetime import datetime

def filePath(folderPath):
    dbutils.fs.mv("/mnt/landing/Customer/" , "/mnt/processed/CustomerData/{}/".format(folderPath) , True)


folderPath = datetime.now().strftime('%m-%d-%Y')

filePath(folderPath)


