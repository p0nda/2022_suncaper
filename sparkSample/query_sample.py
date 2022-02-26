import fn
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import firecalls

df = firecalls.init_db()

# task 1
# sample1 = df.select("IncidentNumber", "AvailableDtTm", "CallType") \
#     .where(" CallType == 'Medical Incident'")
# sample1.show()

# task 2
# df.select("CallType")\
#     .where(df.CallType.isNotNull())\
#     .agg(countDistinct('CallType').alias('num_of_type'))\
#     .show(truncate=False)

# task 3
# df.select("CallType")\
#     .where(df.CallType.isNotNull())\
#     .distinct()\
#     .show(truncate=False)
# 分组分别计数问题

# task 4
# df.withColumnRenamed('Delay', 'ResponseDelayedinMins')\
#     .select('ResponseDelayedinMins')\
#     .where('ResponseDelayedinMins>5')\
#     .show()

# task 5
# sample5=df.withColumn('IncidentDate',to_timestamp(df.CallDate, 'MM/dd/yyyy'))\
#     .drop('CallDate')

# task 6
# sample5.select(year("IncidentDate").alias('year'))\
#     .distinct()\
#     .orderBy(sample5.IncidentDate.asc())\
#     .show(truncate=False)

# task 7
df.select('CallType')\
    .where(df.CallType.isNotNull())\
    .groupBy('CallType')\
    .agg(F.max('NumAlarms').alias('max'))\
    .orderBy(col('count').desc())\
    .show(truncate=False)
