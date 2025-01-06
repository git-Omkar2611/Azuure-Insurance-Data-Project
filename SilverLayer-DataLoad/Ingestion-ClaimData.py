# Databricks notebook source
# MAGIC %md
# MAGIC <b> Remove all where Claim_id , Policy_id , Claim Status , Claim Amount , Last Updated is NULL <b> <br>
# MAGIC <b> Remove all rows where policy id doesnt exists in Policy Table </b>  <br>
# MAGIC <b> Convert dateofclaim to date column with format mm-dd-yyyy </b> <br>
# MAGIC <b> Ensure claim amount is > 0 </b> <br>
# MAGIC <b> Add the merged_date timestamp </b> <br>

# COMMAND ----------

claimData_df = spark.sql("""
                         select  c.claim_id, c.policy_id  , to_date(date_format(date_of_claim,  'MM-dd-yyyy' ) , 'MM-dd-yyyy') as date_of_claim  , claim_amount  , claim_status   , LastUpdatedTimeStamp  from bronzelayer.claim c
                         inner join bronzelayer.policy p
                         on c.policy_id = p.policy_id
                         where claim_amount is not null and claim_id is not null and c.policy_id is not null and claim_status is not null and LastUpdatedTimeStamp is not null and merge_flag = false and c.claim_amount > 0
                         """)
display(claimData_df)

# COMMAND ----------

claimData_df.createOrReplaceTempView('claim_temp')

spark.sql("""
          MERGE INTO silverlayer.claim as T USING claim_temp as S ON t.claim_id = s.claim_id
          WHEN MATCHED THEN UPDATE SET T.policy_id = S.policy_id , T.date_of_claim = S.date_of_claim, T.claim_amount = S.claim_amount, T.claim_status = S.claim_status, T.LastUpdatedTimeStamp = S.LastUpdatedTimeStamp, T.merged_timestamp = current_timestamp()
          WHEN NOT MATCHED THEN INSERT (claim_id,policy_id,date_of_claim,claim_amount,claim_status,LastUpdatedTimeStamp,merged_timestamp) VALUES (S.claim_id,S.policy_id,S.date_of_claim,S.claim_amount,S.claim_status,S.LastUpdatedTimeStamp,current_timestamp())
          """)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE bronzelayer.claim set merge_flag = TRUE where merge_flag = FALSE
# MAGIC