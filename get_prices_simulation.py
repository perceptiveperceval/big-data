
import time
import vnstock
from datetime import date,timedelta, datetime
import pyspark
from pyspark.sql import SparkSession
import requests
import json
from config import SPARK_MASTER, TICKER_NAME, DATA_PATH, RAW_DATA_PATH
from pyspark.sql.functions import udf, col, explode, lit,substring, to_date, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType,TimestampType
from pyspark.sql import Row
from pyspark.context import SparkContext
import os

# doc fd : "2022-12-31"


# tickers = vnstock.listing_companies().ticker
current_time_path = "/home2/hadoop/project/big-data/simulation/current_time.txt"

with open(current_time_path, "r") as f:
    line = f.readlines()[0]  # Y, m, d
    start_date = datetime.strptime(line, "%Y-%m-%d")
    end_date = start_date + timedelta(days=7)

fd = int(time.mktime(start_date.timetuple()))
td = int(time.mktime(end_date.timetuple()))

with open(current_time_path, "w") as f:
    f.write(end_date.strftime("%Y-%m-%d"))


def path_is_readable(spark_session, x):
  try:
    spark_session.read.json(x)
    return True
  except:
    return False


# response function - udf
def executeRestApi(url):
  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
    res = requests.get(url)
  except Exception as e:
    return e

  if res != None and res.status_code == 200:
    return json.loads(res.text)

  return None

#
schema = StructType([
  StructField("ticker", StringType(), True),
  StructField("data", ArrayType(
    StructType([
      StructField("open", FloatType()),
      StructField("high", FloatType()),
      StructField("low", FloatType()),
      StructField("close", FloatType()),
      StructField("volume", IntegerType()),
      StructField("tradingDate", StringType()),
    ])
  ))
])

#
udf_executeRestApi = udf(executeRestApi, schema)

# spark = SparkSession.builder.appName("UDF REST Demo").getOrCreate()
conf = pyspark.SparkConf().setMaster(SPARK_MASTER)\
        .setAppName("Stock Crawling")\
        .set("spark.executor.memory","6g")

sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# auto create directory
if not os.system("hadoop fs -test -d " + RAW_DATA_PATH):
    os.system("hadoop fs -mkdir " + RAW_DATA_PATH)
    os.system("hadoop fs -chmod -R 755 " + RAW_DATA_PATH)

# requests
RestApiRequest = Row("url")
request_df = spark.createDataFrame([
            RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(ticker, fd, td) ) for ticker in [TICKER_NAME]
          ])\
          .withColumn("execute", udf_executeRestApi(col("url")))
request_df_collected = request_df.collect() #write to hdfs

schema = StructType([
      StructField("open_price", FloatType()),
      StructField("high_price", FloatType()),
      StructField("low_price", FloatType()),
      StructField("close_price", FloatType()),
      StructField("volume", IntegerType()),
      StructField("trading_date", StringType())])

for row in request_df_collected:
  queries =spark.createDataFrame(
       row.execute.data,schema).withColumn("ticker_name", lit(row.execute.ticker)) \
       .select("close_price","high_price","low_price","open_price","ticker_name","trading_date","volume")

  
  if path_is_readable(spark, f"{DATA_PATH}/{row.execute.ticker}.json"):
    file = spark.read.json(f"{DATA_PATH}/{row.execute.ticker}.json")
    file_latest_date = file.withColumn("date",to_date(substring('trading_date',0,10))).select('date').rdd.max()[0]
    final = queries.withColumn("date",to_date(substring('trading_date',0,10))).filter(col("date")>file_latest_date).drop("date")
    final.write.mode("append").json(f"{DATA_PATH}/{row.execute.ticker}.json")
  else:
    queries.write.mode("append").json(f"{DATA_PATH}/{row.execute.ticker}.json")

# # spark.stop()
