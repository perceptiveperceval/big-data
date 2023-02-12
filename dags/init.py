
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, date
import os
    
default_args = {'owner': 'airflow','start_date': datetime(2023, 1, 1,1),}


with DAG(
    dag_id='initialization',
    description='crawl data (from beginning to 31/12/2022) using spark and save to hdfs',
    schedule_interval='@never'
) as dag:
    
    
    name = "get_prices_init"


    spark_crawl = SparkSubmitOperator(task_id = "crawl_recent_prices",
                                    application = "/home2/hadoop/project/big-data/init/get_prices_init.py" ,
                                    conn_id = "spark_default",
                                    dag = dag)
