from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bs4 import BeautifulSoup
import pandas as pd



from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from Cricbuzz_files.Extract import web_scrap
from Cricbuzz_files.Transform import clean
from Cricbuzz_files.Load import load_to_db  


dag = DAG(dag_id='Cricbuzz_ETL',
    start_date=datetime(2023,12,7),
    #schedule_interval="0 0 * * *",
    description='This pipline is to get the data from the Cricbuzz site',
    schedule='@daily',
    catchup=False,
)

Create_balls_table = PostgresOperator(
    task_id="Create_balls_table",
    dag = dag,
    postgres_conn_id="Cricbuzz_db",
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
    postgres_conn_id="Cricbuzz_db",
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


Extract = PythonOperator(
    task_id='Extract',
    python_callable=web_scrap,
    dag=dag,
)


transform = PythonOperator(
    task_id='transform',
    python_callable=clean,
    dag=dag,
)

load = PythonOperator(
    task_id = 'load',
    python_callable=load_to_db,
    dag = dag,
)


Extract>> transform >> [Create_balls_table,Create_match_table] >> load