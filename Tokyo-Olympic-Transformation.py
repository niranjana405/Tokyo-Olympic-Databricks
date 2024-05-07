# Databricks notebook source


from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "3d25fb7f-7505-4039-bf78-6b5d59afaeed",
"fs.azure.account.oauth2.client.secret": 'MIV8Q~ToVNA8vRNTSHBRvmc4vAA3UfpulwlLodvc',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/584b6cc1-9740-408a-9796-bd5e8a326233/oauth2/token"}
dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@adlstokyoolympic1.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------


spark

# COMMAND ----------

atheletes=spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/athlete.csv")
coaches=spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/coach.csv")
medal=spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/medal.csv")
gender=spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/gender.csv")
teams=spark.read.format("csv").option("header","true").load("/mnt/tokyoolymic/raw-data/team.csv")

# COMMAND ----------

atheletes.show()

# COMMAND ----------

atheletes.printSchema()

# COMMAND ----------

# Assuming your DataFrame is named 'df'
df = atheletes.drop('_c3', '_c4')



# COMMAND ----------

# Drop columns in place
df.drop('_c3', '_c4', inplace=True)


# COMMAND ----------

atheletes.show()

# COMMAND ----------

# Assuming your DataFrame is named 'df'
df = df.drop('_c3', '_c4')


# COMMAND ----------

atheletes.show()

# COMMAND ----------


atheletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/athlete.csv").select("Name", "NOC", "Discipline")

# COMMAND ----------

atheletes.show()

# COMMAND ----------

atheletes.printSchema()

# COMMAND ----------

gender=gender.withColumn("Female",col("Female").cast(IntegerType()))

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

gender.show()

# COMMAND ----------

gender=gender.withColumn("Male",col("Male").cast(IntegerType()))
gender=gender.withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

gender.show()

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

gender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/gender.csv").select("Discipline", "Male", "Female","Total")

# COMMAND ----------

gender.show()

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

gender=gender.withColumn("Male",col("Male").cast(IntegerType()))
gender=gender.withColumn("Total",col("Total").cast(IntegerType()))
gender=gender.withColumn("Female",col("Female").cast(IntegerType()))


# COMMAND ----------

gender.show()

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

medal.printSchema()

# COMMAND ----------

medal=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/medal.csv")


# COMMAND ----------

medal.printSchema()

# COMMAND ----------

#Find the top countries with the highest no of gold medals
top_gold_medal_countries = medal.orderBy("Gold", ascending=False).select("Team/NOC","Gold").show()


# COMMAND ----------

medal.show()

# COMMAND ----------


# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = gender.withColumn(
    'Avg_Female', gender['Female'] / gender['Total']
).withColumn(
    'Avg_Male', gender['Male'] / gender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

atheletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/athletes")


# COMMAND ----------

coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/coach")
gender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/gender")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/team")
medal.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/medal")

