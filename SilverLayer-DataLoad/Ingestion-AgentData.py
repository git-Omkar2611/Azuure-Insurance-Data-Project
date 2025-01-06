# Databricks notebook source
agentData_df = spark.sql("select * from bronzelayer.agent where  merge_flag = false")
display(agentData_df)                   

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Transformation Logic - Remove all rows where BranchID not Exist in Branch Table , Ensure All the phone have valid 10 digit phone no , Replace all the null email with 'admin@xyz.com' and add the merged_date_time_stamp colun (current_timestamp) </b>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where BranchID not Exist in Branch Table</b>

# COMMAND ----------

from pyspark.sql.functions import *
branch_df = spark.sql("select * from bronzelayer.branch")

#result_join_df = agentData_df.join(branch_df, on = "branch_id" , "inner")

df_result = spark.sql("""
                      select agent.* from bronzelayer.agent
                      inner join bronzelayer.branch
                      on agent.branch_id = branch.branch_id
                      where agent.merge_flag = false
                      """)

display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure All the phone have valid 10 digit phone no</b>

# COMMAND ----------

df_phone = df_result.filter(length(col("agent_phone")) == 10)
display(df_phone)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Replace all the null email with 'admin@xyz.com'</b>

# COMMAND ----------

df_phone.createOrReplaceTempView("agent_temp")

df_email = spark.sql("select agent_id , agent_name , agent_phone , branch_id , create_timestamp , regexp_replace(agent_email , '' , 'admin@xyz.com') as agent_email from agent_temp where agent_email = '' \
                  UNION \
select agent_id , agent_name , agent_phone , branch_id , create_timestamp , agent_email from agent_temp where agent_email != ''               ")

display(df_email)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Add the merged_date_time_stamp colun (current_timestamp) </b>

# COMMAND ----------

#df_final = df_email.withColumn("merged_timestamp" , current_timestamp())
df_email.createOrReplaceTempView('clean_agent')

spark.sql("""
          MERGE INTO silverlayer.agent AS T USING clean_agent AS S ON T.agent_id = S.agent_id 
          WHEN MATCHED THEN UPDATE SET T.agent_Phone = s.agent_phone , T.agent_email = S.agent_email , T.agent_name = s.agent_name , T.branch_id = S.branch_id , T.create_timestamp = S.create_timestamp , T.merged_timestamp = current_timestamp()
          WHEN NOT MATCHED THEN INSERT (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp, merged_timestamp) VALUES (S.agent_id, s.agent_name, S.agent_email, s.agent_phone, S.branch_id, S.create_timestamp, current_timestamp()) 
          """)

#display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Update Merge_Flag to True for all the records which have been inserted in SilverLayer </b>

# COMMAND ----------

spark.sql("""
          UPDATE bronzelayer.agent SET merge_flag = true where merge_flag = false
          """)