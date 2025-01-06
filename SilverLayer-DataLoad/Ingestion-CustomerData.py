# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronzelayer.customer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove Customers where Customer ID is NULL</b>  <br>
# MAGIC <b> Remove records where gender is other than Male/Female </b> <br>
# MAGIC <b> Outlier check at some registration_date > DOB </b>

# COMMAND ----------

df = spark.sql("""
               select * from bronzelayer.customer where merge_flag = false and customer_id is not null and lower(gender) in ('male' , 'female') and CAST(date_of_birth as timestamp) < CAST(registration_date as timestamp)
               """)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Merge the data into silver layer while adding current_timestamp

# COMMAND ----------

df.createOrReplaceTempView("customer_temp")

spark.sql("""
            MERGE INTO silverlayer.Customer as T using customer_temp as S on T.customer_id = S.customer_id
            WHEN MATCHED THEN UPDATE SET t.first_name = S.first_name , T.last_name = S.last_name, T.email = S.email , T.phone = S.phone , T.country= S.country , T.city = S.city , T.registration_date = S.registration_date , T.date_of_birth = S.date_of_birth , T.gender = S.gender , T.merged_timestamp = current_timestamp()   
            WHEN NOT MATCHED THEN INSERT (customer_id , first_name , last_name , email , phone , country , city , registration_date , date_of_birth , gender , merged_timestamp) VALUES (S.customer_id , S.first_name , S.last_name , S.email , S.phone , S.country , S.city , S.registration_date , S.date_of_birth , S.gender , current_timestamp())          
          """)


# COMMAND ----------

# MAGIC %md
# MAGIC <b> Update the bronze layer customer data </b>

# COMMAND ----------


spark.sql("""
          UPDATE bronzelayer.customer set merge_flag = True where merge_flag = FALSE
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.customer