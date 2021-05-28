# Databricks notebook source
# PROBLEM STATEMENT:

# COMMAND ----------

'''
Explore the datasets to perform the following task

Tasks:


1. calculate average meter usage by ACORN(A,B,C,D) group on the month(Jan, Feb, Mar) using 30 mins data. use data available for 2013 and 2014. Plot average monthly usage graph for ACORN A,B,C,D if possible

2. use meter usage data for 2013 and 2014 for ACORN groups A,B,C,D and identify which day of week has the highest average electricity usage

3. for year 2013 please identify which month average usage is maximum across ACORN groups (A,B,C,D)

4. use a weather dataset which is available and evaluate how weather impacts meter electricity usage

6. use the data and try to find one useful insight which will help business to make better decisions

'''

# COMMAND ----------

# CREATING A PYSPARK SESSION

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('meters').getOrCreate()
spark

# COMMAND ----------

# IMPORTING DATA INTO DATAFRAMES FROM DBFS

# COMMAND ----------

df_households = spark.read.csv("dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/informations_households-1.csv", header=True,inferSchema = True)
df_weather_hourly =   spark.read.csv("dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/weather_hourly_darksky-1.csv", header=True,inferSchema = True)
df_acorn = spark.read.csv("dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/acorn_details-2.csv", header=True,inferSchema = True)
df_weather_daily =  spark.read.csv("dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/weather_daily_darksky-1.csv",  header=True,inferSchema = True)

# COMMAND ----------

# CHECK THE SCHEMA FOR DATATYPES AND HAVE A LOOK AT THE HOUSHOLD DATAFRAME

# COMMAND ----------

df_households.printSchema()
df_households.limit(2).toPandas()

# COMMAND ----------

# CHECK THE HOUSEHOLD DATA FRAME TO EXTRACT THE BLOCK FILES WHICH HOLDS THE DATA FOR ACORN-A,ACORN-B,ACORN-C AND ACORN-D.

# COMMAND ----------

# ACORN-A GROUP HAS ITS ENERGY CONSUMPTION FILES IN BLOCK_0,BLOCK_1,BLOCK_2 AND BLOCK_3.

# COMMAND ----------

df_households.filter(df_households['Acorn'] == 'ACORN-A').select('file').distinct().show()

# COMMAND ----------

# ACORN-B GROUP HAS ITS ENERGY CONSUMPTION FILES IN  BLOCK_3.

# COMMAND ----------

df_households.filter(df_households['Acorn'] == 'ACORN-B').select('file').distinct().show()

# COMMAND ----------

# ACORN-C GROUP HAS ITS ENERGY CONSUMPTION FILES IN BLOCK_3,BLOCK_4,BLOCK_5 AND BLOCK_6.

# COMMAND ----------

df_households.filter(df_households['Acorn'] == 'ACORN-C').select('file').distinct().show()

# COMMAND ----------

# ACORN-D GROUP HAS ITS ENERGY CONSUMPTION FILES IN BLOCK_6,BLOCK_7,BLOCK_8 ,BLOCK_9,,BLOCK_10,,BLOCK_11 AND BLOCK_12

# COMMAND ----------

df_households.filter(df_households['Acorn'] == 'ACORN-D').select('file').distinct().show()

# COMMAND ----------

# CHECKING THE WEATHER HOURLY TABLE FOR DATA TYPES AND FIRST TWO ROWS.

# COMMAND ----------

df_weather_hourly.printSchema()
df_weather_hourly.limit(2).toPandas()

# COMMAND ----------

# HERE TIME COLUMN IS STRING. CASTING IT INTO TIMESTAMP AND RENAMING THE COLUMN.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_weather_hourly_new = df_weather_hourly.withColumn('time',to_timestamp(df_weather_hourly['time'],'yyyy-MM-dd HH:mm:ss')).withColumnRenamed('time','Timestamp')

# COMMAND ----------

df_weather_hourly_new.printSchema()

# COMMAND ----------


df_weather_hourly_new.limit(2).toPandas()

# COMMAND ----------

# FROM THE TIMESTAMP COLUMN EXTRACTING YEAR, MONTH AND DATE.

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col

df_weather_hourly_new = df_weather_hourly_new.withColumn('Year1',year(df_weather_hourly_new['Timestamp'])).withColumn('Month',month(df_weather_hourly_new['Timestamp'])).withColumn('Date',dayofmonth
(df_weather_hourly_new['Timestamp']))
df_weather_hourly_new.limit(2).toPandas()

# COMMAND ----------

# EXTRACTING THE NAME OF THE DAY OF THE WEEK AND THE MONTH NAME FROM TIMESTAMP

# COMMAND ----------

df_weather_hourly_new = df_weather_hourly_new.withColumn('Week_day',date_format(col('Timestamp'),'EEEE')). \
withColumn('Month_name',date_format(col('Timestamp'),'MMMM'))
df_weather_hourly_new.limit(2).toPandas()

# COMMAND ----------

# CHECKING THE COLUMNS FOR WEATHER DATA DAILY DATASET AND CHECKING THE SCHEMA.

# COMMAND ----------

df_weather_daily.limit(2).toPandas()

# COMMAND ----------

df_weather_daily.printSchema()

# COMMAND ----------

# CHECKING THE ACORN DATASET FOR THE COLUMNS AND CHECKING THE SCHEMA.

# COMMAND ----------

df_acorn.limit(2).toPandas()

# COMMAND ----------

df_acorn.printSchema()

# COMMAND ----------

'''
READING ALL THE DATASETS OF BLOCKS FROM 1 TO 12( THOSE NEEDED FOR ACORN A,B,C AND D) 
INTO A SINGLE DATASET AND CHECKING THE COLUMNS AND SCHEMA.
'''

# COMMAND ----------

paths_new = ["dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_0.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_1.csv", "dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_2.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_3.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_4.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_5.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_6.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_7.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_8.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_9.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_10.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_11.csv","dbfs:/FileStore/shared_uploads/himadri_88@yahoo.com/block_12.csv"]

df_all_blocks = spark.read.csv(paths_new,header = True, inferSchema = True)

# COMMAND ----------

df_all_blocks.count()

# COMMAND ----------

df_all_blocks.limit(3).toPandas()

# COMMAND ----------

'''
CHECKING THE SCHEMA. 
TSTP IS IN STRING. TO BE CONVERTED INTO TIME STAMP.
energy(kWh/hh) IN STRING. TO BE CASTED TO INTEGER AND THEN RENAMED TO KWh
'''

# COMMAND ----------

df_all_blocks.printSchema()

# COMMAND ----------

df_all_blocks= df_all_blocks.withColumn('tstp',to_timestamp(df_all_blocks['tstp'],'yyyy-MM-dd HH:mm:ss.SSSSSSS')).withColumnRenamed('tstp','Timestamp').withColumnRenamed('energy(kWh/hh)','KWh')
df_all_blocks = df_all_blocks.withColumn('Year',year(df_all_blocks['Timestamp'])).withColumn('Month',month(df_all_blocks['Timestamp'])).withColumn('Day_of_week',dayofweek(df_all_blocks['Timestamp'])) \
.withColumn('Date',dayofmonth(df_all_blocks['Timestamp'])).withColumn('Week_day',date_format(col('Timestamp'),'EEEE')). \
withColumn('Month_name',date_format(col('Timestamp'),'MMMM'))
  
  

# COMMAND ----------

df_all_blocks.printSchema()
df_all_blocks.limit(2).toPandas()

# COMMAND ----------

from pyspark.sql.types import *
df_all_blocks = df_all_blocks.withColumn('KWh',df_all_blocks['KWh'].cast(IntegerType()))
df_all_blocks.printSchema()

# COMMAND ----------

'''
CHECKING ALL THE NULL VALUES PRESENT IN ANY COLUMN.
'''

# COMMAND ----------

from pyspark.sql.functions import *

df_all_blocks.select([count(when(col(c).isNull(),c)).alias (c) for c in df_all_blocks.columns]).toPandas().T


# COMMAND ----------

'''
FILLING THE NULL VALUES WITH THE MEAN OF THAT COLUMN.
'''

# COMMAND ----------

def fill_with_mean(df,include = set()):
  stats = df.agg(*(avg(c).alias(c) for c in df.columns if c in include))
  return df.na.fill(stats.first().asDict())

df_all_blocks_cleaned = fill_with_mean(df_all_blocks,['KWh'])

# COMMAND ----------

df_all_blocks_cleaned.select([count(when(col(c).isNull(),c)).alias (c) for c in df_all_blocks.columns]).toPandas().T

# COMMAND ----------

df_all_blocks_cleaned.limit(5).toPandas()

# COMMAND ----------

'''
JOINING ALL THE BLOCK DATA FRAMES STORED TOGETHER WITH HOUSEHOLDS DATAFRAME ON LCLid.
'''

# COMMAND ----------

df_total = df_all_blocks_cleaned.join(df_households,['LCLid'],how = 'left')
df_total.limit(5).toPandas()

# COMMAND ----------

'''
FOR THE YEAR 2013 AND 2014 CHECKING THE AVG KWh CONSUMPTION OVER THE WEEKDAYS.
'''

# COMMAND ----------

# FOR ACORN -A

# COMMAND ----------

df_Acorn_A = df_total[df_total.Acorn.isin('ACORN-A')][df_total.Year.isin(2013,2014)]
df_Acorn_A.groupBy('Week_day','Day_of_week').mean('KWh').orderBy(col('Day_of_week')).select('Week_day','avg(KWh)').toPandas()

# COMMAND ----------

print('Maximum eneergy consumption for ACORN-A is for the weekday : ')
df_Acorn_A.groupBy('Week_day').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

# FOR ACORN-B

# COMMAND ----------

df_Acorn_B = df_total[df_total.Acorn.isin('ACORN-B')][df_total.Year.isin(2013,2014)]
df_Acorn_B.groupBy('Week_day','Day_of_week').mean('KWh').orderBy(col('Day_of_week')).select('Week_day','avg(KWh)').toPandas()

# COMMAND ----------

print('Maximum eneergy consumption for ACORN-B is for the weekday : ')
df_Acorn_B.groupBy('Week_day').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

# FOR ACORN-C

# COMMAND ----------

df_Acorn_C = df_total[df_total.Acorn.isin('ACORN-C')][df_total.Year.isin(2013,2014)]
df_Acorn_C.groupBy('Week_day','Day_of_week').mean('KWh').orderBy(col('Day_of_week')).select('Week_day','avg(KWh)').toPandas()

# COMMAND ----------

print('Maximum eneergy consumption for ACORN-C is for the weekday : ')
df_Acorn_C.groupBy('Week_day').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

# FOR ACORN-D

# COMMAND ----------

df_Acorn_D = df_total[df_total.Acorn.isin('ACORN-D')][df_total.Year.isin(2013,2014)]
df_Acorn_D.groupBy('Week_day','Day_of_week').mean('KWh').orderBy(col('Day_of_week')).select('Week_day','avg(KWh)').toPandas()

# COMMAND ----------

print('Maximum eneergy consumption for ACORN-D is for the weekday : ')
df_Acorn_D.groupBy('Week_day').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

'''
CHECKING THE MONTH WISE AVERAGE ENERGY CONSUMPTION FOR ACORN A,B,C AND D GROUPS FOR THE YEAR 2013.
'''

# COMMAND ----------

# FOR ACORN-A

# COMMAND ----------

df_Acorn_A1 = df_total[df_total.Acorn.isin('ACORN-A')][df_total.Year.isin(2013)]
df_Acorn_A1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).select('Month_name','avg(KWh)').toPandas()

# COMMAND ----------

print('Highest monthly consumption for ACORN-A in 2013 in the month of :')
df_Acorn_A1.groupBy('Month_name').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

a = df_Acorn_A1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month'))
a.registerTempTable("Monthly_energy_usage_2013")
display(sqlContext.sql("select * from Monthly_energy_usage_2013"))

# COMMAND ----------

# FOR ACORN-B

# COMMAND ----------

df_Acorn_B1 = df_total[df_total.Acorn.isin('ACORN-B')][df_total.Year.isin(2013)]
df_Acorn_B1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).select('Month_name','avg(KWh)').toPandas()

# COMMAND ----------

print('Highest monthly consumption for ACORN-B in 2013 in the month of :')
df_Acorn_B1.groupBy('Month_name').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

df_Acorn_B1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).registerTempTable("Monthly_energy_usage_2013")
display(sqlContext.sql("select * from Monthly_energy_usage_2013"))

# COMMAND ----------



# COMMAND ----------

# FOR ACORN-C

# COMMAND ----------

df_Acorn_C1 = df_total[df_total.Acorn.isin('ACORN-C')][df_total.Year.isin(2013)]
df_Acorn_C1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).select('Month_name','avg(KWh)').toPandas()

# COMMAND ----------

print('Highest monthly consumption for ACORN-C in 2013 in the month of :')
df_Acorn_C1.groupBy('Month_name').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

df_Acorn_C1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).registerTempTable("Monthly_energy_usage_2013")
display(sqlContext.sql("select * from Monthly_energy_usage_2013"))

# COMMAND ----------



# COMMAND ----------

# FOR ACORN-D

# COMMAND ----------

df_Acorn_D1 = df_total[df_total.Acorn.isin('ACORN-D')][df_total.Year.isin(2013)]
df_Acorn_D1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).select('Month_name','avg(KWh)').toPandas()

# COMMAND ----------

print('Highest monthly consumption for ACORN-D in 2013 in the month of :')
df_Acorn_D1.groupBy('Month_name').mean('KWh').sort(col('avg(KWh)').desc()).limit(1).toPandas()

# COMMAND ----------

df_Acorn_D1.groupBy('Month_name','Month').mean('KWh').orderBy(col('Month')).registerTempTable("Monthly_energy_usage_2013")
display(sqlContext.sql("select * from Monthly_energy_usage_2013"))

# COMMAND ----------



# COMMAND ----------

'''
CHECKING THE MONTHLY AVERAGE FOR ACRON A,B,C AND D GROUPS FOR THE YEAR 2013 ONLY FOR THE MONTH JANUARY , FEBRUARY AND MARCH.
'''

# COMMAND ----------

# FOR ACRON-A

# COMMAND ----------

df_Acorn_A2 = df_total[df_total.Acorn.isin('ACORN-A')][df_total.Year.isin(2013,2014)][df_total.Month_name.isin('January','February','March')]
df_Acorn_A2.groupBy('Month_name').mean('KWh').toPandas()

# COMMAND ----------

# FOR ACRON-B

# COMMAND ----------

df_Acorn_B2 = df_total[df_total.Acorn.isin('ACORN-B')][df_total.Year.isin(2013,2014)][df_total.Month_name.isin('January','February','March')]
df_Acorn_B2.groupBy('Month_name').mean('KWh').toPandas()

# COMMAND ----------

# FOR ACRON-C

# COMMAND ----------

df_Acorn_C2 = df_total[df_total.Acorn.isin('ACORN-C')][df_total.Year.isin(2013,2014)][df_total.Month_name.isin('January','February','March')]
df_Acorn_C2.groupBy('Month_name').mean('KWh').toPandas()

# COMMAND ----------

# FOR ACRON-D

# COMMAND ----------

df_Acorn_D2 = df_total[df_total.Acorn.isin('ACORN-D')][df_total.Year.isin(2013,2014)][df_total.Month_name.isin('January','February','March')]
df_Acorn_D2.groupBy('Month_name').mean('KWh').toPandas()

# COMMAND ----------

'''
JOINING THE TOTAL ENERGY CONSUMPTION DATASET WITH THE WEATHER DATASET OVER THE TIMESTAMP COLUMN.
'''

# COMMAND ----------

df_conso = df_total.join(df_weather_hourly_new,['Timestamp'],'left').select(['LCLid','KWh','Year',df_total.Month_name,df_total.Week_day,'Acorn','Acorn_grouped','temperature','summary'])
df_conso.limit(2).toPandas()


# COMMAND ----------

'''
CHECKING THE TOTAL NULL VALUES.
'''

# COMMAND ----------

from pyspark.sql.functions import *

df_conso.select([count(when(col(c).isNull(),c)).alias (c) for c in df_conso.columns]).toPandas().T

# COMMAND ----------

df_conso.select('temperature').count()

# COMMAND ----------

'''
ALMOST 50% OF THE VALUES IN TEMPERATURE COLUMN IS MISSING.
'''

# COMMAND ----------

og_len = df_conso.select('temperature').count()
drop_len = df_conso.select('temperature').na.drop().count()
print("Total Rows Dropped:",og_len-drop_len)
print("Percentage of Rows Dropped", (og_len-drop_len)/og_len)

# COMMAND ----------

'''
CHECKING THE DISTINCT TYPES IN THE SUMMARY COLUMN
'''

# COMMAND ----------

summary = df_conso.select('summary').distinct()
summary.toPandas()

# COMMAND ----------

df_conso.groupBy('summary').mean('temperature','KWh').na.drop().toPandas()

# COMMAND ----------

'''
DROPPING THE NULL VALUES FROM THE TEMPERATURE COLUMN
'''

# COMMAND ----------

climate_vs_power = df_conso.groupBy('summary').mean('KWh','temperature').sort(col('avg(KWh)').desc()).na.drop().toPandas()

# COMMAND ----------

'''
PLOTTING THE ENERGY CONSUMPTION VS THE CLIMATE CONDITION.
'''

# COMMAND ----------

df_conso.groupBy('summary').mean('KWh').registerTempTable("Climate_vs_power_consumption_chart")
display(sqlContext.sql("select * from Climate_vs_power_consumption_chart"))

# COMMAND ----------

'''
PLOTTING THE TEMPERATURE VS THE CLIMATE CONDITION
'''

# COMMAND ----------

df_conso.groupBy('summary').mean('temperature').na.drop().registerTempTable("Climate_vs_temperature_chart")
display(sqlContext.sql("select * from Climate_vs_temperature_chart"))

# COMMAND ----------

'''
PLOTTING AVERAGE POWER CONSUMPTION AND TEMPERATURE WITH RESPECT TO THE CLIMATIC CONDITION.
'''

# COMMAND ----------


df_conso.groupBy('summary').mean('KWh','temperature').na.drop().registerTempTable("Climate_vs_power_consumption_chart")
display(sqlContext.sql("select * from Climate_vs_power_consumption_chart"))

# COMMAND ----------


