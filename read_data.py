import time
import vnstock
from datetime import date,timedelta
from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.functions import udf, col, explode,lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType,TimestampType
from pyspark.sql import Row
from pyspark import SparkContext


spark = SparkSession.builder.appName("Udadmo").getOrCreate()
#read from file
df = spark.read.json("hdfs://node-master:9000/test/SAM.json")
df.show()