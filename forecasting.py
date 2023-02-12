import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import seaborn as sns
import os
from config import SPARK_MASTER, TICKER_NAME, FORCASTING_PATH, DATA_PATH
import prophet
from datetime import timedelta
from pyspark.sql.functions import pandas_udf, PandasUDFType


def fill_nan_linear(arr: np.ndarray):
    """Fill nan values in a 1D array using linear interpolation."""
    arr = arr.copy()
    nan_idx = np.isnan(arr)
    arr[nan_idx] = np.interp(np.flatnonzero(nan_idx), np.flatnonzero(~nan_idx), arr[~nan_idx])
    return arr

def fill_nan_df(df, column: str):
    """Fill nan values in a column of a dataframe using linear interpolation"""
    df_pd = df.toPandas()
    df_pd[column] = fill_nan_linear(df_pd[column].values)
    return spark.createDataFrame(df_pd)

def plot_result(df_train: pd.DataFrame, df_forecast: pd.DataFrame, title: str = None, save_postfix: str = None):
    global TICKER_NAME, FORCASTING_PATH
    # reset plot
    plt.clf()
    save_postfix = save_postfix if save_postfix else (title if title else "")
    sns.set(rc={'figure.figsize':(11, 4)})
    df_train.set_index("ds").y.plot(linewidth=0.5, c="blue")
    df_forecast.set_index("ds").yhat.plot(linewidth=0.5, c="red")
    plt.fill_between(df_forecast.ds, df_forecast.yhat_lower, df_forecast.yhat_upper, alpha=0.25, color="blue", label='90% confidence interval')
    # remove margin fill between
    if title:
        plt.title(title)
    # save image
    os.makedirs(FORCASTING_PATH + "/" + TICKER_NAME, exist_ok=True)
    plt.legend()
    plt.savefig(FORCASTING_PATH + "/" + TICKER_NAME + "/"  + f"{save_postfix}.png")


def prepare_data(df, date_col, id_col, y_col):
    df_train = df.select(date_col, id_col, y_col)
    df_train = df_train.withColumnRenamed(y_col, "y")
    df_train = df_train.withColumnRenamed(date_col, "ds")
    df_train = fill_nan_df(df_train, "y")
    return df_train


conf = pyspark.SparkConf().setMaster(SPARK_MASTER)\
        .setAppName("Stock Forcasting")\
        .set("spark.executor.memory","6g")
# sc = SparkContext.getOrCreate(conf=conf)
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Read data
df = spark.read.json(DATA_PATH + "/" + TICKER_NAME + ".json")
df = df.withColumn("trading_date", to_utc_timestamp("trading_date", "GMT+7"))
df = df.withColumn("trading_date", df["trading_date"].cast(DateType()))
df = df.withColumnRenamed("ticker_name", "unique_id").sort("trading_date")
# drop duplicate
df = df.dropDuplicates(["trading_date"])

print(df.show(5))

# cleaning
min_date = df.agg({"trading_date": "min"}).collect()[0][0]
max_date = df.agg({"trading_date": "max"}).collect()[0][0]
print("Min date: ", min_date)
print("Max date: ", max_date)

dates = [x + 1 for x in range(0, (int((max_date - min_date).days) + 1))]
dates = [min_date + timedelta(days=x) for x in dates]
dates_array = [d.strftime("%Y-%m-%d") for d in dates]
df_dates = spark.createDataFrame(dates_array, StringType()).withColumnRenamed("value", "trading_date")
df_dates = df_dates.withColumn("trading_date", to_utc_timestamp("trading_date", "GMT+7"))
df_dates = df_dates.withColumn("trading_date", df_dates["trading_date"].cast(DateType()))

df_filled = df_dates.join(df, on=["trading_date"], how="left")
mode_value = df_filled.groupBy("unique_id").count().sort(desc("count")).collect()[0][0]
df_filled = df_filled.fillna(mode_value, subset=["unique_id"])
print(df_filled.show(10))

schema = StructType([
                    #  StructField('unique_id', StringType()),
                     StructField('ds', TimestampType()),
                     StructField('yhat', DoubleType()),
                     StructField('yhat_upper', DoubleType()),
                     StructField('yhat_lower', DoubleType()),
])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def prophet_udf(df):
    m = prophet.Prophet(
        interval_width=0.90,
        growth='linear',
        changepoint_prior_scale=0.1,
        n_changepoints=30,
        seasonality_prior_scale=15,
        seasonality_mode='multiplicative'
    )
    m.fit(df)
    future = m.make_future_dataframe(periods=7 * 8, freq='d', include_history=True)
    forecast = m.predict(future)
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]


forecast_attr = ["close_price", "high_price", "low_price", "open_price", "volume"]
# forecast_attr = ["close_price"]
print("total_data", df_filled.count())
for attr in forecast_attr:
    df_train = prepare_data(df_filled, "trading_date", "unique_id", attr)
    print("train", df_train.count())
    df_forecast = df_train.groupby("unique_id").apply(prophet_udf)
    print("forecast_size", df_forecast.count())
    plot_result(df_train.toPandas(), df_forecast.toPandas(), title=attr)