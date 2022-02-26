from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

def init_db():
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('HelloSpark') \
        .getOrCreate()

    df = spark.read.json('business_db.json')
    return df