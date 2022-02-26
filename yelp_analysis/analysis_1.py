from pyspark.sql.functions import *

import init_db

df=init_db.init_db()

# task 1
# df.select('name')\
#     .groupby('name')\
#     .agg(count('name').alias('count'))\
#     .orderBy(col('count').desc())\
#     .show(20)

# task 2
# df.select('city','name')\
#     .groupby('city')\
#     .agg(count('name').alias('number'))\
#     .orderBy(col('number').desc())\
#     .show(10)

# task 3
# df.select('state', 'name') \
#     .groupby('state') \
#     .agg(count('name').alias('number')) \
#     .orderBy(col('number').desc()) \
#     .show(5)

# task 4
# df.select('name','stars')\
#     .groupby('name')\
#     .agg(count('name').alias('count'),avg('stars').alias('avg_star'))\
#     .orderBy(col('count').desc())\
#     .select('name','avg_star')\
#     .show(20)

# task 5
df.select('city', 'stars')\
    .groupby('city')\
    .agg(avg('stars').alias('avg_star'))\
    .orderBy(col('avg_star').desc())\
    .select('city','avg_star')\
    .show(10)