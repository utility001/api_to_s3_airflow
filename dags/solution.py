from airflow.models import DAG
from airflow.operators.python import PythonOperator

from utils import full_pipeline

# from datetime import datetime, timedelta

# Default args
default_args = {
    "owner": "Samson"
}

# Instantiate the DAG
my_dag = DAG(
    dag_id="from_api_to_s3",
    description="This DAG will get randomuser data from randomuserapi and push them to S3 bucket",

    # TODO: Scheduling
    # schedule=,
    # schedule_interval=
    # start_date=,
    # end_date=
)

# Write the tasks
task1 = PythonOperator(
    task_id = "full_pipeline",
    python_callable=full_pipeline,
    dag = my_dag
)

# pipeline
task1