# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType
data2 = [("James","joe","Smith","36636","M",3000, "9978804955"),
    ("Michael","Rose","Smith","40288","M",4000, "9993803495"),
    ("Robert","Reese","Williams","42114","M",4000, "9993804955"),
    ("Maria","Anne","Jones","39192","F",4000, "9956804955"),
    ("Jen","Mary","Brown","99110","F",5000, "9343804955")
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True), \
    StructField("mobile", StringType(), True) \
  ])
 
df = spark.createDataFrame(data=data2, schema=schema)
df.createOrReplaceTempView("employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE raw.hr.employees AS
# MAGIC SELECT firstname, middlename, lastname, id, gender, salary, base64(aes_encrypt(mobile, 'abcdefabcdefabcdefabcdef', 'ECB')) as phone FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM raw.hr.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP VIEW IF EXISTS raw.hr.employees_protected;
# MAGIC CREATE VIEW raw.hr.employees_protected as
# MAGIC SELECT firstname, middlename, lastname, id, gender, salary, 
# MAGIC       CASE WHEN
# MAGIC         is_account_group_member('restricted_access') THEN cast(aes_decrypt(unbase64(phone),
# MAGIC                           'abcdefabcdefabcdefabcdef', 'ECB') AS STRING)
# MAGIC         ELSE phone
# MAGIC       END AS phone
# MAGIC FROM raw.hr.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM raw.hr.employees_protected;
