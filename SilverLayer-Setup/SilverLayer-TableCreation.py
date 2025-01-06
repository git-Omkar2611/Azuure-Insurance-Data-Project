# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC Create or Replace table silverlayer.Agent (agent_id  integer , 
# MAGIC agent_name  string , 
# MAGIC agent_email string ,
# MAGIC agent_phone string ,
# MAGIC branch_id  integer ,
# MAGIC create_timestamp  timestamp,
# MAGIC merged_timestamp timestamp
# MAGIC  )
# MAGIC USING DELTA LOCATION '/mnt/silverlayer/Agent'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC Create or Replace table silverlayer.Branch (branch_id  integer , 
# MAGIC branch_country  string , 
# MAGIC branch_city string ,
# MAGIC merged_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/silverlayer/Branch'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC Create or Replace table silverlayer.Customer (branch_id  integer , 
# MAGIC customer_id int ,
# MAGIC     first_name string , 
# MAGIC     last_name string , 
# MAGIC     email string, 
# MAGIC     phone  string, 
# MAGIC     country string ,
# MAGIC     city string, 
# MAGIC     registration_date timestamp , 
# MAGIC     date_of_birth timestamp ,
# MAGIC     gender string ,
# MAGIC     merged_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/silverlayer/CustomerData'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC Create or Replace table silverlayer.Customer (branch_id  integer , 
# MAGIC customer_id int ,
# MAGIC     first_name string , 
# MAGIC     last_name string , 
# MAGIC     email string, 
# MAGIC     phone  string, 
# MAGIC     country string ,
# MAGIC     city string, 
# MAGIC     registration_date timestamp , 
# MAGIC     date_of_birth timestamp ,
# MAGIC     gender string ,
# MAGIC     merged_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/silverlayer/CustomerData'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE or Replace TABLE silverlayer.Claim (claim_id int , 
# MAGIC policy_id  int , 
# MAGIC date_of_claim DATE ,
# MAGIC claim_amount double ,
# MAGIC claim_status  string ,
# MAGIC LastUpdatedTimeStamp  timestamp ,
# MAGIC merged_timestamp TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/silverlayer/Claim'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE or Replace TABLE silverlayer.Policy (policy_id int , 
# MAGIC policy_type  string ,
# MAGIC customer_id int ,
# MAGIC start_date timestamp , 
# MAGIC end_date timestamp ,
# MAGIC premium double , 
# MAGIC coverage_amount double ,
# MAGIC merged_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/silverlayer/Policy'