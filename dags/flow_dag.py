
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import *
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, date
import os


default_args = {'owner': 'airflow',}


with DAG(
    dag_id='complete_flow',
    description='Complete flow, ran weekly, crawl all data and predict/visualize SAM index',
    start_date=datetime(2023, 2, 19),
    schedule_interval= '@weekly'
) as dag:
    
    crawl_spark_job = SparkSubmitOperator(task_id = "get_prices",
                                    application = "/home2/hadoop/project/big-data/complete/get_prices_complete.py",
                                    conn_id = "spark_default",
                                    dag = dag)
    predict_spark_job = SparkSubmitOperator(task_id = "spark_default",
                                    application = "forecasting.py",
                                    conn_id = "spark_default",
                                    dag = dag)
    visualize_spark_job = SparkSubmitOperator(task_id = "visualize",
                                        application = "visualize.py",
                                        conn_id = "spark_default",
                                        dag = dag)
    
    crawl_spark_job >> [predict_spark_job, visualize_spark_job]