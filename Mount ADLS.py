# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <b>Bronze Layer Mounting</b>

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format('bronzelayer', 'insuranceprojstorageacc'),mount_point = '/mnt/bronzelayer',extra_configs={
        'fs.azure.sas.{}.{}.blob.core.windows.net'.format('bronzelayer', 'insuranceprojstorageacc'):'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-29T19:04:45Z&st=2024-08-03T11:04:45Z&spr=https&sig=1SF4m5Mj0P%2FlNxtVPbaWmesb7NcGKXoRW6ajTL8Daj8%3D'
    })

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <b>Landing Container Mounting</b>

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format('landing', 'insuranceprojstorageacc'),mount_point = '/mnt/landing',extra_configs={
        'fs.azure.sas.{}.{}.blob.core.windows.net'.format('landing', 'insuranceprojstorageacc'):'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-29T19:04:45Z&st=2024-08-03T11:04:45Z&spr=https&sig=1SF4m5Mj0P%2FlNxtVPbaWmesb7NcGKXoRW6ajTL8Daj8%3D'
    })

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <b>Processed Container Mounting</b>

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format('processed', 'insuranceprojstorageacc'),mount_point = '/mnt/processed',extra_configs={
        'fs.azure.sas.{}.{}.blob.core.windows.net'.format('processed', 'insuranceprojstorageacc'):'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-29T19:04:45Z&st=2024-08-03T11:04:45Z&spr=https&sig=1SF4m5Mj0P%2FlNxtVPbaWmesb7NcGKXoRW6ajTL8Daj8%3D'
    })

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mounting Silver Layer Container </b>

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format('silverlayer', 'insuranceprojstorageacc'),mount_point = '/mnt/silverlayer',extra_configs={
        'fs.azure.sas.{}.{}.blob.core.windows.net'.format('silverlayer', 'insuranceprojstorageacc'):'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-29T19:04:45Z&st=2024-08-03T11:04:45Z&spr=https&sig=1SF4m5Mj0P%2FlNxtVPbaWmesb7NcGKXoRW6ajTL8Daj8%3D'
    })

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mounting Gold Layer Container </b>

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format('goldlayer', 'insuranceprojstorageacc'),mount_point = '/mnt/goldlayer',extra_configs={
        'fs.azure.sas.{}.{}.blob.core.windows.net'.format('goldlayer', 'insuranceprojstorageacc'):'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-29T19:04:45Z&st=2024-08-03T11:04:45Z&spr=https&sig=1SF4m5Mj0P%2FlNxtVPbaWmesb7NcGKXoRW6ajTL8Daj8%3D'
    })