from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils import fetch_user_info, normalize_profiles, load_df_to_s3
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    "owner": "Samson",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=2)
}

@dag(dag_id="taskflow_from_api_to_s3",
     default_args=default_args,
    #  start_date=datetime(2025, 3, 20),
    #  schedule_interval="@daily"
    )
def push_to_s3():

    @task
    def fetch_from_api():
        return fetch_user_info()
    
    @task
    def transform_to_df(inp):
        return normalize_profiles(profiles=inp)
    
    @task
    def load_to_s3(df):
        return load_df_to_s3(df=df)

    extract = fetch_from_api()
    transform = transform_to_df(extract)
    load = load_to_s3(transform)



# Now create an instance of the dag
to_s3_dag = push_to_s3()