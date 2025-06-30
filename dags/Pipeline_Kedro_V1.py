from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from dotenv import load_dotenv
from pymongo import MongoClient



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2024, 6 ,27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Kedro_FetchData_BELIB',
    default_args=default_args,
    description='Récupère les données Belib et les insère dans MongoDB toutes les 2 minutes',
    schedule='*/5 * * * *',  # Chaque 30 minutes
    catchup=False,
    tags=["belib", "mongodb", "api", "kedro"],
) as dag:

    task_run_pipeline = BashOperator(
        task_id="Kedro_FetchData_BELIB_Task",
        bash_command="cd /home/qqssdd12/pedro && kedro run"
    )