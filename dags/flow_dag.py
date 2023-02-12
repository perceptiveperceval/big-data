
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, date
import os
    
default_args = {'owner': 'airflow','start_date': datetime(2023, 1, 1,1),}


with DAG(
    dag_id='flow',
    description='Workflow',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily'
) as dag:
    path = os.getcwd()
    name = "get_prices.py"
    for root, dirs, files in os.walk(path):
        if name in files:
            app_path = (os.path.join(root, name))
            break

    spark_job = SparkSubmitOperator(task_id = "get_prices",
                                    application = app_path,
                                    conn_id = "spark_default",
                                    dag = dag)
