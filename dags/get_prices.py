
import time
import vnstock
from datetime import date,timedelta
from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.functions import udf, col, explode, lit,substring, to_date, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType,TimestampType
from pyspark.sql import Row
from pyspark import SparkContext

tickers = vnstock.listing_companies().ticker
fd = int(time.mktime(time.strptime((date.today()- timedelta(days=30)).strftime("%Y-%m-%d"), "%Y-%m-%d")))
td = int(time.mktime(time.strptime((date.today()- timedelta(days=3)).strftime("%Y-%m-%d"), "%Y-%m-%d")))

def path_is_readable(spark_session, x):
  try:
    spark_session.read.parquet(x)
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

spark = SparkSession.builder.appName("UDF REST Demo").getOrCreate()

# requests
RestApiRequest = Row("url")
request_df = spark.createDataFrame([
            RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(ticker, fd, td) ) for ticker in ["SAM"]
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

  
  if path_is_readable(spark, "hdfs://node-master:9000/test/{}.json".format(row.execute.ticker)):
    file = spark.read.json("hdfs://node-master:9000/test/{}.json".format(row.execute.ticker))
    file_latest_date = file.withColumn("date",to_date(substring('trading_date',0,10))).select('date').rdd.max()[0]
    final = queries.withColumn("date",to_date(substring('trading_date',0,10))).filter(col("date")>file_latest_date).drop("date")
    final.write.mode("append").json("hdfs://node-master:9000/test/{}.json".format(row.execute.ticker))
  else:
    queries.write.mode("append").json("hdfs://node-master:9000/test/{}.json".format(row.execute.ticker))

# # spark.stop()

