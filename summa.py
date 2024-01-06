from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'your_username',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'summa2',
    default_args=default_args,
    description='Your DAG description',
    schedule_interval=None,  # Run daily
)

# Define your tasks
task1 = DummyOperator(task_id='task1', dag=dag)

bash_script = "summa.sh"
copy_files_to_hadoop = BashOperator(
    task_id="copy_files_to_hadoop",
    bash_command="bash summa.sh",
    dag=dag,
)

# Set up task dependencies
task1 >> copy_files_to_hadoop
