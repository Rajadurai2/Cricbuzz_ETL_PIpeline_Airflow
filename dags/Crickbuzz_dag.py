from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook 
#from airflow.providers.apache.hdfs.hooks import webhdfs. as h

#import airflow.providers.apache.hdfs.hooks as hdfs 
# hiiiiiiiiii
import json

from datetime import datetime,timedelta

match_url = [["https://www.cricbuzz.com/cricket-full-commentary/75437/ind-vs-aus-5th-match-icc-cricket-world-cup-2023",'https://www.cricbuzz.com/cricket-full-commentary/66183/srh-vs-rr-4th-match-indian-premier-league-2023']]

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from Cricbuzz_files.Extract import  Scrap_multiple_sites
from Cricbuzz_files.Transform import Transform_multiple_files
from Cricbuzz_files.Load import load_multiple_files

def condition_to_skip(**kwargs):
    execution_date = kwargs['ds']
    with open('schedule.json', 'r') as json_file:
        data_dict = json.load(json_file)
    
    print(type(data_dict))

    print("file = ",data_dict)
    test_dates = {'2023-12-28': ['https://www.cricbuzz.com/cricket-full-commentary/66169/gt-vs-csk-1st-match-indian-premier-league-2023'],
                    '2024-01-01': ['https://www.cricbuzz.com/cricket-full-commentary/66173/pbks-vs-kkr-2nd-match-indian-premier-league-2023',
                                    'https://www.cricbuzz.com/cricket-full-commentary/66176/lsg-vs-dc-3rd-match-indian-premier-league-2023'],
                    '2024-01-03': ['https://www.cricbuzz.com/cricket-full-commentary/66183/srh-vs-rr-4th-match-indian-premier-league-2023'],}
    
    match_dates = data_dict

    print("execution_date =",execution_date)
    if execution_date in match_dates.keys(): 
        kwargs['ti'].xcom_push(key=execution_date, value=match_dates[execution_date])
        return 'run_today'
    
    else:
        print(help(WebHDFSHook))
        return 'skip_today'
    
    # Implement your condition based on the execution date



dag = DAG(dag_id='Cricbuzz_ETL',
    start_date = datetime(2024,1,1),
    schedule_interval=None,
    schedule='@daily',
    description='This pipline is to get the data from the Cricbuzz site',
    default_args={
        'retries': 1,
    '   retry_delay': timedelta(seconds=20),
    }
    #schedule='@daily',
    #catchup=False,
)

Create_balls_table = PostgresOperator(
    task_id="Create_balls_table",
    dag = dag,
    postgres_conn_id="CRICBUZZ_DB",
    retries=None,
    sql="""
        CREATE TABLE IF NOT EXISTS Ball_by_ball_data(
            "match_id" NUMERIC ,
            "ball" TEXT,
            "Commentary Text" TEXT,
            "innings" TEXT,
            "batting_team" TEXT,
            "bowling_team"TEXT,      
            "Batsman" TEXT,
            "Bowler" TEXT,
            "runs" TEXT, 
            "extra" TEXT, 
            "ball_length" TEXT, 
            "ball_line" TEXT,
            "ball_speed" TEXT,
            "shot_name" TEXT, 
            "ball_destination" TEXT, 
            "out_type" TEXT 
            )""",
)

Create_match_table = PostgresOperator(
    task_id="Create_match_table",
    dag = dag,
    retries=None,
    postgres_conn_id="CRICBUZZ_DB",
    sql="""
        CREATE TABLE IF NOT EXISTS Match_data(
        
        "match_id" NUMERIC PRIMARY KEY,
        "match_no" TEXT,
        "date" TEXT,
        "series" TEXT,
        "season" TEXT, 
        "venue" TEXT, 
        "team_a" TEXT, 
        "team_b" TEXT,
        "team_a_11" TEXT, 
        "team_b_11" TEXT, 
        "team_a_subs" TEXT, 
        "team_b_subs" TEXT,
        "toss" TEXT,
        "toss_winner" TEXT,
        "toss_choosen" TEXT,
        "winner" TEXT, 
        "player_of_the_match" TEXT
        )""",
)


Get_schedule = BranchPythonOperator(
    task_id='Get_schedule',
    provide_context=True,
    python_callable=condition_to_skip,
    dag=dag,
)

skip_today = BashOperator(
    task_id='skip_today',
    bash_command = 'echo "skipped task"',
    dag=dag,
)

run_today = BashOperator(
    task_id='run_today',
    bash_command = 'echo "running task"',
    dag=dag,
)

# trigger_task = TriggerDagRunOperator(
#     task_id='trigger_task',
#     trigger_dag_id='Cricbuzz_ETL',
#     conf={'match_urls': '{{ ti.xcom_pull(task_ids="branch_task") }}'},
#     dag=dag,
# )

Extract = PythonOperator(
    task_id='Extract',
    retries=1,
    retry_delay= timedelta(seconds=20),
    python_callable=Scrap_multiple_sites,
    op_args = match_url,
    dag=dag,
)


transform = PythonOperator(
    task_id='transform',
    retries=None,
    python_callable=Transform_multiple_files,
    op_args = match_url,
    dag=dag,
)

load_to_postgres = PythonOperator(
    task_id = 'load_to_postgres',
    trigger_rule='all_done',
    retries=0,
    python_callable=load_multiple_files,
    dag = dag,
)
def load_to_hadoop(match_url):
    file_name=match_url.split('/')[-1]
    print(file_name)

    webHDFS_hook = WebHDFSHook(webhdfs_conn_id="HADOOP_CONNECTION")
    client = webHDFS_hook.get_conn()
    print(client)
    local_file_path = f'./Scraped__raw_files/{file_name}.csv'

    # HDFS destination path
    hdfs_destination_path = '/IPL_2023/Scraped__raw_files/'

    # Use the hook to copy the file to HDFS
    webHDFS_hook.load_file(local_file_path, hdfs_destination_path)

    webHDFS_hook.load_file(f"./Transformed_files/cleaned_{file_name}.csv",'/IPL_2023/Transformed_files/')


    print("------------------------------------Success ---------------------------------------------")
    # client.<operation>

def load_files_to_hadoop(**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['ds']
    # Retrieve the result from XCom
    match_url = ti.xcom_pull(task_ids='Get_schedule', key=execution_date)

    print(match_url)
    print(type(match_url))
    if len(match_url)>1:
        print("working")
        for link in match_url :
            print(f'-------------------------{link}--loading_to_db_started----------------------------------------')
            load_to_hadoop(link)
            print(f'---------------------------------{link}loading_to_db_started ------------------------------')
    elif len(match_url) == 1:
        print("1-link",match_url[0])

        load_to_hadoop(match_url[0])
    
    else :
        print("errrrrrrorrrrrrrrrrrrrrrrrrrr.......................")



transfer_task = PythonOperator(
    task_id='transfer_to_hdfs',
    trigger_rule='all_done',
    python_callable = load_files_to_hadoop,
    provide_context = True,
    dag=dag,
)

Get_schedule >>[run_today,skip_today]

run_today >> Extract >> transform >> [Create_balls_table,Create_match_table] >> load_to_postgres >> transfer_task