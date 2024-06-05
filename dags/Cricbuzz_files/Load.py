def load_to_db(url):
    #url = "https://www.cricbuzz.com/cricket-full-commentary/75437/ind-vs-aus-5th-match-icc-cricket-world-cup-2023"
    import pandas as pd
    from datetime import datetime
    from sqlalchemy import create_engine

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    file_name=url.split('/')[-1]
    

    full_data = pd.read_csv(f"Transformed_files/cleaned_{file_name}.csv")

    date_string = full_data['date'][0]

    date_object = datetime.strptime(date_string, '%b %d,%Y')

    number_date_format = date_object.strftime('%m/%d/%Y')

    match_id = number_date_format.replace("/","")
    full_data['match_id'] = match_id

    ball_by_ball_data = full_data[['match_id',"ball","Commentary Text",'innings', 'batting_team', 'bowling_team','Batsman', 'Bowler', 'runs', 'extra', 'ball_length', 'ball_line','ball_speed','shot_name','ball_destination', 'out_type']]


    match_data = full_data[['match_id','match_no','date',  'series','season', 'venue','team_a', 'team_b','team_a_11', 'team_b_11', 'team_a_subs', 'team_b_subs', 'toss' ,'toss_winner', 'toss_choosen','winner', 'player_of_the_match']]
    
    match_data = match_data.head(1)
    
    conn_string = 'postgresql+psycopg2://rajabala:rajabala@POSTGRES_CRICBUZZ_DB/Crickbuzz'
    
    db = create_engine(conn_string)
    conn = db.connect()

    ball_by_ball_data.to_sql(name='ball_by_ball_data', con=conn, index=False, if_exists='append')

    match_data.to_sql(name='match_data',con=conn,index=False, if_exists='append')
 


def load_multiple_files(**kwargs):
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
            load_to_db(link)
            print(f'---------------------------------{link}loading_to_db_started ------------------------------')
    elif len(match_url) == 1:
        print("1-link",match_url[0])

        load_to_db(match_url[0])
    
    else :
        print("errrrrrrorrrrrrrrrrrrrrrrrrrr.......................")
