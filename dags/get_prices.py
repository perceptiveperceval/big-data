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
fd = int(time.mktime(time.strptime((date.today()- timedelta(days=3)).strftime("%Y-%m-%d"), "%Y-%m-%d")))
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

# spark = SparkSession.builder.appName("UDF REST Demo").getOrCreate()
sc = SparkContext("local", "First App")
# requests
RestApiRequest = Row("url", "body")
request_df = sc.createDataFrame([
            RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(ticker, fd, td) , body) for ticker in tickers
          ])\
          .withColumn("execute", udf_executeRestApi(col("url"), col("body")))
request_df_collected = request_df.select(col('execute.ticker'),explode(col("execute.data")).alias("data"))\
    .select(col('data.volume'),
            col("ticker"), 
            col("data.close"),
            col("data.open"),
            col("data.high"),
            col("data.low"),
            col("data.tradingDate")) \
    .withColumnRenamed("ticker","ticker_name") \
    .withColumnRenamed("open","open_price")\
    .withColumnRenamed("high","high_price")\
    .withColumnRenamed("low","low_price")\
    .withColumnRenamed("close","close_price")\
    .withColumnRenamed("tradingDate","trading_date")\
    .collect() #write to hdfs

schema = StructType([
      StructField("volume", IntegerType()),
      StructField("ticker_name", StringType(), True),
      StructField("open_price", FloatType()),
      StructField("high_price", FloatType()),
      StructField("low_price", FloatType()),
      StructField("close_price", FloatType()),
      StructField("trading_date", StringType())])

for row in request_df_collected:
  sc.createDataFrame(row).write.json("/content/stock/"+row.ticker+".json",mode = "append")

# spark.stop()

