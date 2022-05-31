from airflow import DAG
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator



with DAG(dag_id='Kranio_ETL', default_args=default_arg, schedule_interval='@once') as dag:
    start = DummyOperator(task_id='start')
    notebook_task1 = PapermillOperator(task_id="ETL_SPARK_TRANSFORM",input_nb="C:/Users/Usurio/Desktop/kronio/TransformPySpark.ipynb",
        output_nb="C:/Users/Usurio/Desktop/kronio/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"})
    notebook_task2 = PapermillOperator(task_id="PANDAS_LOAD_VISUZ",input_nb="C:/Users/Usurio/Desktop/kronio/PandasLoad.ipynb",
        output_nb="C:/Users/Usurio/Desktop/kronio/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"})


notebook_task1 >> notebook_task2

