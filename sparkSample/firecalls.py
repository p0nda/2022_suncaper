from pyspark.shell import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType


def init_db():
    fire_schema = StructType([StructField("CallNumber", IntegerType(), True),
                              StructField("UnitID", StringType(), True),
                              StructField("IncidentNumber", IntegerType(), True),
                              StructField("CallType", StringType(), True),
                              StructField("CallDate", StringType(), True),
                              StructField("WatchDate", StringType(), True),
                              StructField("CallFinalDisposition", StringType(), True),
                              StructField("AvailableDtTm", StringType(), True),
                              StructField("Address", StringType(), True),
                              StructField("City", StringType(), True),
                              StructField("Zipcode", IntegerType(), True),
                              StructField("Battalion", StringType(), True),
                              StructField("StationArea", StringType(), True),
                              StructField("Box", StringType(), True),
                              StructField("OriginalPriority", StringType(), True),
                              StructField("Priority", StringType(), True),
                              StructField("FinalPriority", IntegerType(), True),
                              StructField("ALSUnit", BooleanType(), True),
                              StructField("CallTypeGroup", StringType(), True),
                              StructField("NumAlarms", IntegerType(), True),
                              StructField("UnitType", StringType(), True),
                              StructField("UnitSequenceInCallDispatch", IntegerType(), True),
                              StructField("FirePreventionDistrict", StringType(), True),
                              StructField("SupervisorDistrict", StringType(), True),
                              StructField("Neighborhood", StringType(), True),
                              StructField("Location", StringType(), True),
                              StructField("RowID", StringType(), True),
                              StructField("Delay", FloatType(), True)]
                             )
    sampleDF = spark \
        .read \
        .schema(fire_schema) \
        .option("header", True) \
        .csv("sf-fire-calls.txt")
    return sampleDF
