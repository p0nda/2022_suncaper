from pyspark.sql.functions import to_timestamp, col, month, year, desc

import init_db

df=init_db.init_db()

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
sampleNeighbor=df.select('Neighborhood')\
    .where("Neighborhood=='San Francisco'")\
    .show(10,truncate=False)
