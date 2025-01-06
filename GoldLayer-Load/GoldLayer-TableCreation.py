# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales by Policy Type and Month : </b> This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE goldlayer.sales_by_policy_type_and_month(
# MAGIC   policy_type string ,
# MAGIC   sale_month string,
# MAGIC   total_premium integer ,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/goldlayer/sales_by_policy_type_and_month'

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Claim By Policy Type and Status : </b> This table would contain number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE goldlayer.claims_by_policy_type_and_status(
# MAGIC   policy_type string ,
# MAGIC   claim_status string,
# MAGIC   total_claims integer,
# MAGIC   total_claims_amount DOUBLE ,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/goldlayer/claims_by_policy_type_and_status'

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Analyze the claim data based on the policy type like AVG , MAX , MIn , Count of claim </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table goldlayer.claims_analysis(
# MAGIC   policy_type string ,
# MAGIC   claim_status string ,
# MAGIC   avg_claim_amount DOUBLE ,
# MAGIC   max_claim_amount DOUBLE ,
# MAGIC   min_claim_amount DOUBLE ,
# MAGIC   total_claims integer ,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC ) USING DELTA LOCATION '/mnt/goldlayer/claims_analysis'