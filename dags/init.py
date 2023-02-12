
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, date
import os
    
default_args = {'owner': 'airflow','start_date': datetime(2023, 1, 1,1),}


with DAG(
    dag_id='initialization',
    description='push crawled data (from beginning to 31/12/2022) from local to hdfs and crawl the rest using spark',
    schedule_interval='@once'
) as dag:
    
    
    path = os.getcwd()
    name = "get_prices.py"
    for root, dirs, files in os.walk(path):
        if name in files:
            app_path = (os.path.join(root, name))
            break

    upload_file = PythonOperator()


    spark_crawl = SparkSubmitOperator(task_id = "crawl_recent_prices",
                                    application = "init/get_prices_init.py" ,
                                    conn_id = "spark_default",
                                    dag = dag)
