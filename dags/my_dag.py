from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum



dag = DAG(dag_id='my_dag',
    start_date=datetime(2023, 9 ,9),
    description='Our first DAG with ETL process!',
    schedule='@daily',
)

from random import randint # Import to generate random numbers

def _training_model():
  return randint(1, 10) # return an integer between 1 - 10


training_model_tasks = [
    PythonOperator(
      task_id=f"training_model_{model_id}",
      dag = dag,
      python_callable=_training_model,
      op_kwargs={
        "model": model_id
      }
    ) for model_id in ['A', 'B', 'C']
  ]

def _choose_best_model(ti):
  accuracies = ti.xcom_pull(task_ids=[
    'training_model_A',
    'training_model_B',
    'training_model_C'
  ])
  if max(accuracies) > 8:
    return 'is_accurate'
  return 'is_inaccurate'

choosing_best_model = BranchPythonOperator(
  task_id="choose_best_model",
  python_callable=_choose_best_model,
  dag=dag
)

is_accurate = BashOperator(
   task_id="is_accurate",
   bash_command="echo 'accurate'"
)

is_inaccurate = BashOperator(
  task_id="is_inaccurate",
  bash_command="echo 'inaccurate'"
)

training_model_tasks >> choosing_best_model >> [is_accurate, is_inaccurate]