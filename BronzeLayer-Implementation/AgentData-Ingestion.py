# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = "agent_id int , \
agent_name  string , \
agent_email string ,\
agent_phone string ,\
branch_id  integer ,\
create_timestamp  timestamp "


agentData_df = spark.read.parquet('/mnt/landing/AgentData/*.parquet' ,schema = schema , header = True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Added Merge_Flag to ensure if the data is merged in silver layer </b>

# COMMAND ----------

from pyspark.sql.functions import *
df_with_flag = agentData_df.withColumn("merge_flag" , lit(False))
df_with_flag.write.option("path","/mnt/bronzelayer/Agent").mode("append").saveAsTable("bronzelayer.agent")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronzelayer.agent

# COMMAND ----------

from datetime import datetime

current_time = datetime.now().strftime('%m-%d-%Y')

dbutils.fs.mv("/mnt/landing/AgentData/" , "/mnt/processed/AgentData/{}/".format(current_time) , True)

# COMMAND ----------

