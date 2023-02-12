
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import *
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, date
import os
from config import DATA_PATH, RAW_DATA_PATH


default_args = {'owner': 'airflow',}


with DAG(
    dag_id='flow_simulation',
    description='Simulation flow, ran after init, visualize/predict on 5/2-12/2 data',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@never'
) as dag:

    crawl_spark_job = SparkSubmitOperator(task_id = "get_prices_simulation",
                                    application = "get_prices_simulation.py",
                                    conn_id = "spark_default",
                                    dag = dag)
    predict_spark_job = SparkSubmitOperator(task_id = "get_prices_simulation",
                                    application = "forecasting_simulation.py",
                                    conn_id = "spark_default",
                                    dag = dag)
    visualize_spark_job = SparkSubmitOperator(task_id = "visualize_simulation",
                                        application = "visualize_simulation.py",
                                        conn_id = "spark_default",
                                        dag = dag)