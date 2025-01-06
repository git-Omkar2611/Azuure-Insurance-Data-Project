# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS bronzelayer;
# MAGIC USE bronzelayer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES