from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from ETL.etl_function import api,tratarPlanilhaCepea,tratarPlanilhaBoiGordo
# inicializando o default_args
default_args={
    'owner': 'Rafael',
    'retries': 1
}
# Objeto Dag
with DAG(
    dag_id = 'dag_etl',
    default_args=default_args,
    start_date = datetime(2023, 1, 25),
    schedule_interval="@daily",
    catchup = False
) as dag:

# Tasks são implementadas após o objeto Dag
# PythonOperator é uma task que executa comandos python
    task1 = PythonOperator(
        task_id = 'request_api',
        python_callable = api,
    )

    task2 = PythonOperator(
        task_id = 'planilha_cepea',
        python_callable = tratarPlanilhaCepea,
    )

    task3 = PythonOperator(
        task_id = 'planilha_boi_gordo',
        python_callable = tratarPlanilhaBoiGordo,
        provide_context=True
    )
    # Definindo a dependência das Tasks
    [task1,task2] >> task3