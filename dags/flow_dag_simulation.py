
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import *
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, date
import os


default_args = {'owner': 'airflow',}


with DAG(
    dag_id='flow_simulation',
    description='Simulation flow, ran after init, visualize/predict on 5/2-12/2 data',
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/5 * * * *"
) as dag:

    crawl_spark_job = SparkSubmitOperator(task_id = "get_prices_simulation",
                                    application = "/home2/hadoop/project/big-data/get_prices_simulation.py",
                                    conn_id = "spark_default",
                                    dag = dag)
    predict_spark_job = SparkSubmitOperator(task_id = "forecasting_simulation",
                                    application = "/home2/hadoop/project/big-data/forecasting.py",
                                    conn_id = "spark_default",
                                    dag = dag)
    visualize_spark_job = SparkSubmitOperator(task_id = "visualize_simulation",
                                        application = "/home2/hadoop/project/big-data/visualize.py",
                                        conn_id = "spark_default",
                                        dag = dag)


    crawl_spark_job >> [predict_spark_job, visualize_spark_job]
    # crawl_spark_job >> predict_spark_job