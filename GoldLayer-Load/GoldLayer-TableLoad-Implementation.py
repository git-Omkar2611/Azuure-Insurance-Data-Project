# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales by Policy Type and Month : </b> This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temp view vw_sales_by_policy_type_and_month 
# MAGIC as
# MAGIC select p.policy_type , date_trunc('month', p.start_date) as sale_month , SUM(p.premium) as total_premium from silverlayer.policy p
# MAGIC where p.policy_type is not null
# MAGIC group by p.policy_type , date_trunc('month', p.start_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO goldlayer.sales_by_policy_type_and_month as T USING vw_sales_by_policy_type_and_month as S ON t.policy_type = S.policy_type and t.policy_type = S.policy_type and T.sale_month = S.sale_month
# MAGIC WHEN MATCHED THEN UPDATE SET T.total_premium = S.total_premium , T.updated_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (  policy_type  ,  sale_month ,  total_premium  ,  updated_timestamp ) VALUES( S.policy_type,  S.sale_month ,  S.total_premium , current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Claim By Policy Type and Status : </b> This table would contain number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE temp view vw_gold_claims_by_policy_type_and_status 
# MAGIC as
# MAGIC select p.policy_type , c.claim_status , COUNT(c.claim_id) as total_claims , SUM(c.claim_amount) as total_claims_amount
# MAGIC from silverlayer.claim c
# MAGIC   inner join silverlayer.policy p
# MAGIC     on c.policy_id=  p.policy_id
# MAGIC where p.policy_type is not null
# MAGIC   GROUP BY p.policy_type , c.claim_status 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO goldlayer.claims_by_policy_type_and_status as T USING vw_gold_claims_by_policy_type_and_status as S ON t.policy_type = S.policy_type and t.claim_status = S.claim_status
# MAGIC WHEN MATCHED THEN UPDATE SET T.total_claims = S.total_claims , T.total_claims_amount = S.total_claims_amount , T.updated_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (  policy_type  ,  claim_status ,  total_claims ,  total_claims_amount  ,  updated_timestamp ) VALUES( S.policy_type, S.claim_status, S.total_claims, S.total_claims_amount, current_timestamp())

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC <b> Analyze the claim data based on the policy type like AVG , MAX , MIn , Count of claim </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temp view vw_gold_claims_analysis as
# MAGIC select p.policy_type , avg(c.claim_amount) as avg_claim_amount , MAX(c.claim_amount) as max_claim_amount , MIN(c.claim_amount) as min_claim_amount , COUNT(distinct c.claim_id) as total_claims from silverlayer.claim c
# MAGIC inner join silverlayer.policy p
# MAGIC on c.policy_id = p.policy_id
# MAGIC where p.policy_type is not null
# MAGIC GROUP BY p.policy_type 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO goldlayer.claims_analysis as T USING vw_gold_claims_analysis as S ON T.policy_type = S.policy_type
# MAGIC WHEN MATCHED THEN UPDATE SET T.avg_claim_amount = S.avg_claim_amount , T.max_claim_amount = S.max_claim_amount , T.min_claim_amount = S.min_claim_amount , T.total_claims = S.total_claims , T.updated_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT( policy_type, avg_claim_amount, max_claim_amount, min_claim_amount, total_claims, updated_timestamp ) VALUES( S.policy_type, S.avg_claim_amount, S.max_claim_amount, S.min_claim_amount, S.total_claims, current_timestamp() )