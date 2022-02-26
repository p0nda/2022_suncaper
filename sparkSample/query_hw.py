from pyspark.sql.functions import *
import datetime
from pyspark.ml.stat import Correlation
import firecalls

df = firecalls.init_db()

# task 1
# df.select('CallType')\
#     .distinct()\
#     .show(truncate=False)

# task 2
# sample2=df.withColumn('IncidentDate', to_timestamp(col('CallDate'), 'MM/dd/yyyy')) \
#     .drop('CallDate') \
#     .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")) \
#     .drop("WatchDate") \
#     .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")) \
#     .drop("AvailableDtTm")
#
# sample2.select(month('IncidentDate').alias('IncidentMonth'))\
#     .where(year('IncidentDate').__eq__(2018))\
#     .groupby('IncidentMonth')\
#     .count()\
#     .orderBy(desc("count"))\
#     .show(truncate=False)

# task 3
# sampleNeighbor=df.select('Neighborhood')\
#     .where("City =='SAN FRANCISCO'")\
#     .groupBy('Neighborhood')\
#     .agg(count('Neighborhood').alias('count'))\
#     .orderBy(col('count').desc())\
#     .show(truncate=False)

# task 4
# df.select('Neighborhood','Delay')\
#     .where("City =='SAN FRANCISCO'")\
#     .groupBy('Neighborhood')\
#     .agg(max('Delay').alias('MaxDelay'))\
#     .orderBy(col('MaxDelay').desc())\
#     .show(10)

# task 5
# sample5 = df.withColumn('IncidentWeek', weekofyear(to_timestamp(col('CallDate'), "MM/dd/yyyy"))) \
#     .withColumn('IncidentDate', to_timestamp(col('CallDate'), "MM/dd/yyyy")) \
#     .drop('CallDate')
# sample5.select('IncidentWeek', 'IncidentDate')\
#     .where(year('IncidentDate').__eq__(2018))\
#     .groupBy('IncidentWeek')\
#     .agg(count('IncidentWeek').alias('count'))\
#     .orderBy(col('count').desc())\
#     .show(truncate=False)

# task 6
# df['Neighborhood']=df['Neighborhood'].astype('category').cat.codes
# df['Zipcode']=df['Zipcode'].astype('category').cat.codes
# df.stat.corr('Neighborhood','Zipcode')

df=df.select('Neighborhood', 'Zipcode', 'NumAlarms')
r1=Correlation.corr(df,'Neighborhood','Zipcode')

