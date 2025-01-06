# Databricks notebook source
branchData_df = spark.sql("select * from bronzelayer.branch")

display(branchData_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Remove all where Branch_ID IS NULL

# COMMAND ----------

df = spark.sql("select * from bronzelayer.branch where branch_id is not null and merge_flag = False")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Remove all the leading and trailing space in Branch Country and convert it into Upper Case </b>

# COMMAND ----------

df.createOrReplaceTempView("branch_temp")

df_spaces_removed = spark.sql("""
                              SELECT b.branch_id , b.branch_city , upper(trim(b.branch_country)) FROM bronzelayer.branch b where branch_id  is not null and b.merge_flag = False 
                              """)

display(df_spaces_removed)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge into Silver Layer Table</b>

# COMMAND ----------

spark.sql("""
          MERGE INTO silverlayer.branch AS T using branch_temp as S on T.branch_id = S.branch_id
          WHEN MATCHED THEN UPDATE SET T.branch_city = S.branch_city , T.branch_country = S.branch_country , T.merged_timestamp = current_timestamp() 
          WHEN NOT MATCHED THEN INSERT (branch_id , branch_city , branch_country , merged_timestamp) VALUES (S.branch_id , S.branch_city , S.branch_country , current_timestamp())
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Upate the merge_flag in bronzelayer table </b>

# COMMAND ----------

spark.sql("""
            UPDATE bronzelayer.branch SET merge_flag = True WHERE merge_flag = False
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.branch