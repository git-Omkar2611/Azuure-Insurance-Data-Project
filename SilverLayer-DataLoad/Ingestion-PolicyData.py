# Databricks notebook source
# MAGIC %md
# MAGIC <b> Remove all rows where Customerid , policyid , agentid is null </b> <br>
# MAGIC <b> Remove all rows where AgentID not exists in Agent Table </b> <br>
# MAGIC <b> Remove all rows where CustomerID not exists in Customer Table </b> <br>
# MAGIC <b> Remove all rows where CustomerID not exists in Customer Table </b> <br>

# COMMAND ----------

policyData_df = spark.sql("""
                          select p.policy_id  , p.policy_type  , p.customer_id  , p.start_date  , p.end_date  , p.premium , p.coverage_amount  from bronzelayer.policy p
                          inner join bronzelayer.customer c
                          on p.customer_id = c.customer_id
                          where policy_id is not NULL and 
                          p.customer_id is not null and 
                          p.policy_id is not null and 
                          p.merged = False and
                          p.premium > 0 and p.coverage_amount > 0 and p.end_date > p.start_date
                          """)

display(policyData_df)

# COMMAND ----------

policyData_df.createOrReplaceTempView('policy_temp')

spark.sql(
    """
    MERGE INTO silverlayer.policy AS T USING policy_temp AS S on T.policy_id = S.policy_id
    WHEN MATCHED THEN UPDATE SET  T.policy_type = S.policy_type  , T.customer_id = S.customer_id  , T.start_date = S.start_date  , T.end_date = S.end_date  , T.premium = S.premium , T.coverage_amount = S.coverage_amount , T.merged_timestamp = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (policy_id  , policy_type  ,customer_id  ,start_date  , end_date  , premium , coverage_amount , merged_timestamp) VALUES(S.policy_id  , S.policy_type  , S.customer_id  , S.start_date  , S.end_date  , S.premium , S.coverage_amount , current_timestamp)
    """
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE bronzelayer.policy set merged = true WHERE merged = FALSE