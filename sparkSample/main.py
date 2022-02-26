# from pyspark.sql import SparkSession
#
# spark = SparkSession \
#     .builder \
#     .appName("Python spark SQL") \
#     .getOrCreate()
#
# df = spark.createDataFrame([('tom', 20), ('jack', 40)], ['name', 'age'])
#
# df.select('name').show()

import query_sample

query_sample