import time
import vnstock
from datetime import date,timedelta
from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType,TimestampType
from pyspark.sql import Row
from pyspark import SparkContext

tickers = vnstock.listing_companies().ticker
fd = int(time.mktime(time.strptime((date.today()- timedelta(days=10)).strftime("%Y-%m-%d"), "%Y-%m-%d")))
td = int(time.mktime(time.strptime((date.today()- timedelta(days=2)).strftime("%Y-%m-%d"), "%Y-%m-%d")))

body = json.dumps({
})

# response function - udf
def executeRestApi(url, body):
  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
    res = requests.get(url, data=body)
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
RestApiRequest = Row("url", "body")
request_df = spark.createDataFrame([
            RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(ticker, fd, td) , body) for ticker in tickers[:2]
          ])\
          .withColumn("execute", udf_executeRestApi(col("url"), col("body")))
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
       .select("close_price","high_price","low_price","open_price","ticker_name","trading_date","volume").collect()
  try:
    collect_file_df = spark.read.json("hdfs://node-master:9000/test/{}.json".format(row.execute.ticker)).collect()
  except:
    collect_file_df = [row]
  finally:
    final = spark.createDataFrame(collect_file_df + queries)
    final.drop_duplicates(subset = ['trading_date']) \
    .write.mode("overwrite").json("hdfs://node-master:9000/test/{}.json".format(row.execute.ticker))
