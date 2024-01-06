def clean(url):
    #url = "https://www.cricbuzz.com/cricket-full-commentary/75437/ind-vs-aus-5th-match-icc-cricket-world-cup-2023" 
    file_name=url.split('/')[-1]
    import pandas as pd
    import re
    import requests
    from bs4 import BeautifulSoup
    df=pd.read_csv(f'Scraped__raw_files/{file_name}.csv')


    def short_name(team):
            short_name={
                # IPL teams
                'Chennai Super Kings':'CSK',
        'Mumbai Indians':'MI',
        'Gujarat Titans':'GT',
        'Kolkata Knight Riders':'KKR',
        'Punjab Kings':'PBKS',
        'Sunrisers Hyderabad':'SRH',
        'Rajasthan Royals':'RR',
        'Lucknow Super Giants':'LSG',
        'Delhi Capitals':'DC',
        'Delhi Daredevils':'DD',
        'Royal Challengers Bangalore':'RCB',
        'Kings XI Punjab':'PBKS',
        'Rising Pune Supergiant':'RPS',
        'Gujarat Lions':'GL',
        # International teams 
                
        'Australia': 'AUS',
        'Bangladesh': 'BAN',
        'England': 'ENG',
        'India': 'IND',
        'New Zealand': 'NZ',
        'Pakistan': 'PAK',
        'South Africa': 'SA',
        'Sri Lanka': 'SL',
        'West Indies': 'WI',
        'Afghanistan': 'AFG',
        'Ireland': 'IRE',
        'Zimbabwe': 'ZIM',
                
                
                        
        }
            return short_name[team]

    def find_toss():
        toss=df['toss'][0]
        toss_win=toss[:toss.find('have')]
        toss_choose=toss.split(' ')[-1]
        
        df['toss_winner']=short_name(toss_win.strip())
        df['toss_choosen']=toss_choose
        
        
    def find_bowling_team():
        toss_winner=df['toss_winner'][0]
        toss_choose=df['toss_choosen'][0]
        a_team=short_name(df['team_a'][0])
        b_team=short_name(df['team_b'][0])
        teams=[a_team,b_team]
        
        
        for i in teams:
            if i==toss_winner:
                teams.remove(i)
        opt=teams[0]
        
        print(toss_winner,opt)
        if toss_choose=='bat':
            print('hi')
            df.loc[df['innings']=='1st Inns','batting_team']=toss_winner
            df.loc[df['innings']=='1st Inns','bowling_team']=opt
            df.loc[df['innings']=='2nd Inns','batting_team']=opt
            df.loc[df['innings']=='2nd Inns','bowling_team']=toss_winner
        if toss_choose=='field':
            df.loc[df['innings']=='1st Inns','batting_team']=opt
            df.loc[df['innings']=='1st Inns','bowling_team']=toss_winner
            df.loc[df['innings']=='2nd Inns','batting_team']=toss_winner
            df.loc[df['innings']=='2nd Inns','bowling_team']=opt
        else:
            print(toss_choose)
            
    def find_innings():
        toss_winner=df['toss_winner'][0]
        toss_choose=df['toss_choosen'][0]
        

        if toss_choose=='field':
            
            df['innings'].mask(df['innings']!=f'{toss_winner} Inns'.strip(),'1st Inns',inplace=True)
            df['innings'].mask(df['innings']==f'{toss_winner} Inns'.strip(),'2nd Inns',inplace=True)
            
        else:
            df['innings'].mask(df['innings']!=f'{toss_winner} Inns'.strip(),'2nd Inns',inplace=True)
            df['innings'].mask(df['innings']==f'{toss_winner} Inns'.strip(),'1st Inns',inplace=True)
                            
    def find_batsman_bowler():
        
        df['Batsman']=''
        df['Bowler']=''
        for i,row in enumerate(df['Commentary Text'].values):
            text=row.split(',')[0]
            text=text.split(' to ')
            batsman=text[1]
            bowler=text[0]
            df.loc[i,['Batsman']]=batsman
            #df['Batsman'][i]=batsman
            df.loc[i,['Bowler']]=bowler

    texts=[]
    #find the runs by ball 
    def find_runs():
        
        df['runs']=''
        df['extra']=''
        for i,comment in enumerate(df['Commentary Text'].values):
            text=comment.split(',')[1].strip().lower()
            #print('text=',text)
            texts.append(text)
            if text.startswith('1 run'.casefold()):
                df.loc[i,['runs']]= 1
                df.loc[i,['extra']]= 0
            elif text.startswith('2 runs'.casefold()):
                text= 2
                df.loc[i,['runs']]= 2
                df.loc[i,['extra']]= 0
            elif text.startswith('no run'.casefold()):
                df.loc[i,['runs']]= 0
                df.loc[i,['extra']]= 0
            elif text.startswith('3 runs'.casefold()):
                df.loc[i,['runs']]= 3
                df.loc[i,['extra']]= 0
            elif text.startswith('FOUR'.casefold()):
                df.loc[i,['runs']]= 4
                df.loc[i,['extra']]= 0
            elif text.startswith('SIX'.casefold()):
                df.loc[i,['runs']]= 6
                df.loc[i,['extra']]= 0
            elif text.startswith(('byes'.casefold(),'leg'.casefold())):
                if str(comment.split(',')[2]).lstrip().lower()=='four':
                    df.loc[i,['extra']]= 4
                elif str(comment.split(',')[2]).lstrip().lower()=='six':
                    df.loc[i,['extra']]= 6
                elif str(comment.split(',')[2]).lstrip().lower()=='1 run':
                    df.loc[i,['extra']]= 1
                elif str(comment.split(',')[2]).lstrip().lower()=='2 runs':
                    df.loc[i,['extra']]= 2
                elif str(comment.split(',')[2]).lstrip().lower()=='3 runs':
                    df.loc[i,['extra']]= 3
                else:
                    df.loc[i,['extra']]=0
                df.loc[i,['runs']]= 'byes'
            elif text.startswith('no ball'.casefold()):
                if str(comment.split(',')[2]).lstrip().lower()=='four':
                    df.loc[i,['extra']]= '1+4'
                elif str(comment.split(',')[2]).lstrip().lower()=='six':
                    df.loc[i,['extra']]='1+6'
                elif str(comment.split(',')[2]).lstrip().lower()=='1 run':
                    df.loc[i,['extra']]= 1
                elif str(comment.split(',')[2]).lstrip().lower()=='2 runs':
                    df.loc[i,['extra']]= 2
                elif str(comment.split(',')[2]).lstrip().lower()=='3 runs':
                    df.loc[i,['extra']]= 3
                else:
                    df.loc[i,['extra']]=1
                df.loc[i,['runs']]= 'no ball'
            elif text.startswith('out'.casefold()):
                df.loc[i,['runs']]= 'out'
                df.loc[i,['extra']]= 'out'
            elif text.startswith('wide'.casefold()):
                df.loc[i,['runs']]= 'wide'
                df.loc[i,['extra']]= 1
            elif text.startswith('2 wides'.casefold()):
                df.loc[i,['runs']]= 'wide'
                df.loc[i,['extra']]= 2
            elif text.startswith('3 wides'.casefold()):
                df.loc[i,['runs']]= 'wide'
                df.loc[i,['extra']]= 3
            elif text.startswith('4 wides'.casefold()):
                df.loc[i,['runs']]= 'wide'
                df.loc[i,['extra']]= 4
            elif text.startswith('5 wides'.casefold()):
                df.loc[i,['runs']]= 'wide'
                df.loc[i,['extra']]= 5
        

    def find_length():
        for i,line in enumerate(df["Commentary Text"]):
            match=re.search('short|slot|full-toss|back of|good|tosse|full|on a length|yorker|of a length|length delivery|length ball|length on|length,',line,re.IGNORECASE)

            length_balls=['on a length','of a length','length delivery','length ball','length on','length,']
        
            if match:
                
                if match.group(0).casefold()=='short':
                    df.loc[i,['ball_length']]= 'Short'
                elif match.group(0).casefold()=='full':
                    df.loc[i,['ball_length']]= 'Full'
                elif match.group(0).casefold()=='slot':
                    df.loc[i,['ball_length']]= 'Slot'
                elif match.group(0).casefold()=='back of':
                    df.loc[i,['ball_length']]= 'Back of Length'
                elif match.group(0).casefold()=='good':
                    df.loc[i,['ball_length']]= 'Good'
                elif match.group(0).casefold()=='tosse' or match.group(0).casefold()=='full-toss':
                    df.loc[i,['ball_length']]= 'Full toss'
                elif match.group(0).casefold()=='yorker':
                    df.loc[i,['ball_length']]= 'yorker'
                elif  match.group(0).casefold() in length_balls:
                    df.loc[i,['ball_length']]= 'Good'
                else:
                    print(match.group(0))
                    break

            else:
                df.loc[i,['ball_length']]= None    

    def find_line():
        k=0
        m=0
        for i,line in enumerate(df["Commentary Text"]):
            if df.loc[i,'ball_length'] == None:
                
                match=re.search('down-leg|wide|at the stump|off stump|outside-off|on the pad|outside-leg|outside off|on middle|on the stumps|the hip|sticks|into the wicket|outside leg|to off|on off|on leg|leg stump|leg-lined|middle-lined',line,re.IGNORECASE)
                if match:
                    m=m+1
                    #print(match)
                    stumps=['at the stumps','on middle','on the stumps','the hip','sticks','into the wicket','leg stump','down leg','middle-lined','at the stump','on the pad']

                    off_side=['off stump','outside-off','outside off','on off','on off','to off']
                    leg_side=['outside leg','on leg','leg stump','leg-lined','outside-leg']


                    if match.group(0).casefold() in stumps:
                        df.loc[i,['ball_line']]= 'stump line'
                    elif match.group(0).casefold() in off_side:
                        df.loc[i,['ball_line']]= 'off side'
                    elif match.group(0).casefold() in leg_side:
                        df.loc[i,['ball_line']]= 'leg side'
                    elif match.group(0).casefold() == 'wide':
                        df.loc[i,['ball_line']]= 'wide' 
                    else:
                        
                        print('error in ball line ',match.group(0))
                else:
                    df.loc[i,['ball_line']]= None
            else:
                
                match=re.search('down-leg|wide|at the stump|off stump|outside-off|outside-leg|on the pad|outside off|on middle|on the stumps|the hip|sticks|into the wicket|outside leg|to off|on off|on leg|leg stump|leg-lined|middle-lined',line,re.IGNORECASE)
                if match:
                    #print(match)
                    m=m+1
                    stumps=['at the stump','on middle','on the stumps','the hip','sticks','into the wicket','leg stump','down leg','down-leg','middle-lined','at the stump','on the pad']
                    off_side=['off stump','outside-off','outside off','on off','on off','to off']
                    leg_side=['outside leg','on leg','leg stump','leg-lined','outside-leg']


                    if match.group(0).casefold() in stumps:
                        df.loc[i,['ball_line']]= 'stump line'
                    elif match.group(0).casefold() in off_side:
                        df.loc[i,['ball_line']]= 'off side'
                    elif match.group(0).casefold() in leg_side:
                        df.loc[i,['ball_line']]= 'leg side'
                    elif match.group(0).casefold() == 'wide':
                        df.loc[i,['ball_line']]= 'wide' 
                    else:
                        print('error in ball line ',match.group(0))
                else:
                    
                    df.loc[i,['ball_line']]= None
        
        
    def find_destination():    
        for i,line in enumerate(df["Commentary Text"]):
            #print(line)
            fielding_positions = re.search(r"(?i)\b(?:gully|slip|third\s*man|fine\s*leg|down the pitch|square\s*leg|mid\s*wicket|mid-wicket|deep backward square|long-on|long\s*on|long\s*off|long-off|cover|mid\s*off|mid-off|mid\s*on|mid-on|point|deep square|deep\s*square\s*leg|deep\s*mid\s*wicket|deep\s*cover|deep\s*point|short\s*leg|silly\s*point|short\s*mid\s*wicket|backward\s*point|extra\s*cover|short\s*third\s*man|long\s*stop|leg\s*slip|short\s*fine\s*leg|short\s*mid\s*off|short\s*mid\s*on|cow\s*corner|deep\s*backward\s*point|deep backward square|deep\s*extra\s*cover|deep\s*mid\s*off|deep\s*mid\s*on|deep\s*silly\s*point|fine\s*leg\s*slip|fine\s*third\s*man|long\s*leg|long\s*silly\s*point|long\s*leg\s*slip|short\s*cover|short\s*extra\s*cover|short\s*long\s*leg|short\s*mid\s*leg|short\s*square\s*leg|super\s*cover|super\s*deep\s*extra\s*cover|sweeper|defence|super\s*extra\s*cover|super\s*long\s*off|super\s*long\s*on|super\s*mid\s*off|super\s*mid\s*on|super\s*point|super\s*short\s*cover|covers|super\s*square\s*leg|super\s*third\s*man|off-side|keeper|straight|short fine|square|super\s*mid\s*wicket)\b", line, flags=re.IGNORECASE)
            if fielding_positions:
                df.loc[i,'ball_destination']=fielding_positions.group(0)
            else:
                df.loc[i,'ball_destination']=None

    def find_shots():
        for i,line in enumerate(df["Commentary Text"]):
            if df.loc[i,'runs']=="wide":
                df.loc[i,'shot_name']="wide"
            elif 'edge' in line.lower():
                df.loc[i,'shot_name']="edge"
            elif 'beaten' in line.lower():
                df.loc[i,'shot_name']='beaten'
            elif 'miss' in line.lower():
                df.loc[i,'shot_name']='beaten'
            elif 'block' in line.lower():
                df.loc[i,'shot_name']='defend'
            elif 'defend' in line.lower():
                df.loc[i,'shot_name']='defend'
            elif 'drive' in line.lower():    
                if df.loc[i,'ball_destination'] in ['long-on','mid-on','midwicket','mid-wicket','deep midwicket']:
                    df.loc[i,'shot_name']='on drive'
                    
                elif df.loc[i,'ball_destination'] in ['long-off','mid-off']:
                    df.loc[i,'shot_name']='off drive'
                elif df.loc[i,'ball_destination'] in ['cover','deep extra cover','covers','extra cover']:
                    df.loc[i,'shot_name']='cover drive'
                elif df.loc[i,'ball_destination'] in ['deep square','deep backward square','square leg','backward point','square']:
                    df.loc[i,'shot_name']='square drive'

                elif df.loc[i,'ball_destination'] =='straight':
                    df.loc[i,'shot_name']='straight drive'
                else:
                    df.loc[i,'shot_name']='drive'
            elif 'pull' in line.lower():
                df.loc[i,'shot_name']='pull shot'
            elif 'hook' in line.lower():
                df.loc[i,'shot_name']='hook shot'
            elif 'cut' in line.lower():
                #print(df.loc[i,'ball_destination'])
                if df.loc[i,'ball_destination'] in ['Third man','third man','short third man']:
                    df.loc[i,'shot_name']='late cut'
                elif df.loc[i,'ball_destination'] in ['backward point','point']:
                    df.loc[i,'shot_name']='Cut'
                elif df.loc[i,'ball_destination'] in ['deep cover','deep point','cover']:
                    df.loc[i,'shot_name']='square cut'
                else:
                    df.loc[i,'shot_name']='cut shot' 
            elif 'flick' in line.lower():
                df.loc[i,'shot_name']='flick shot'
            elif 'push' in line.lower():
                df.loc[i,'shot_name']='push shot'
            elif 'switch' in line.lower():
                df.loc[i,'shot_name']='switch hi'
            elif 'helicopter' in line.lower():
                df.loc[i,'shot_name']='helicopter shot'
            elif 'ramp' in line.lower():
                df.loc[i,'shot_name']='ramp shot'
            elif 'upper' in line.lower():
                df.loc[i,'shot_name']='upper cut'
            elif 'slog' in line.lower():
                df.loc[i,'shot_name']='slog shot'

            else:
                shots_regex = r"drive|Cover Drive|Square Drive|On Drive|Pull Shot|Hook Shot|Cut Shot|Late Cut|Sweep Shot|Reverse Sweep|Flick Shot|Glance|Square Cut|Block|Defensive Shot|Leave|Nurgle|Slog|Switch Hit|Dilscoop|Helicopter Shot|Ramp Shot|Upper Cut|Slap Shot|Inside-out Shot|Scoop Shot|Dab|Reverse Dab|Leg Glance|Hockey Shot|Marillier Shot|Reverse Marillier Shot|Sweep Scoop|Yoga Shot|Helicopter Flick|Upper Hook|Slog Sweep|Inside-out Hook|Slog Reverse Sweep|Upper Drive|Pull Drive|Sweep Drive|Sweep Pull|Sweep Flick|Scoop Flick|Flick Drive|Switch Flick|Dilscoop Drive|Slap Drive|Slap Hook|Slap Pull|Slap Flick|Reverse Slap|Reverse Slap Drive|Reverse Slap Hook|Reverse Slap Pull|Reverse Slap Flick|Slap Sweep|Slap Scoop|Slap Cut|Slap Reverse Sweep|Reverse Slap Sweep|Reverse Slap Scoop|Reverse Slap Cut|Reverse Slap Reverse Sweep"
                match=re.search(shots_regex,line,re.IGNORECASE)
                if match:
                    df.loc[i,'shot_name']=match.group(0)
    def find_speed():
        l=0
        for i,line in enumerate(df["Commentary Text"]):
            match = re.search(r'([\d.]+)kph',line)
            l=l+1
            if match:
                speed = match.group(1)
                #print(line)
                df.loc[i,'ball_speed']=match.group(1)
            else:
                df.loc[i,'ball_speed']=None



    def find_out_type():
        for i,line in enumerate(df["Commentary Text"]):
            if df.loc[i,'runs']=='out':
                if 'caught' in line.lower():
                    try:
                        text = line.lower()
                        left = 'caught by'
                        right = '!!'
                        df.loc[i,'out_type']='Catch out ,'+text[text.index(left)+len(left):text.index(right)]
                    except:
                        df.loc[i,'out_type']='Catch out ,'+df.loc[i,'Bowler']
                elif 'run out' in line.lower():
                    match=re.search(r"\((.*?)\)",line,re.IGNORECASE)
                    if match:
                        df.loc[i,'out_type']='run out ,'+match.group(0)
                    else:
                        df.loc[i,'out_type']='run out ,'
                elif 'bowled!!' in line.lower():
                    df.loc[i,'out_type']='bowled'
                elif 'lbw!!' in line.lower():
                    df.loc[i,'out_type']='LBW'
                elif 'Hit Wkt!!'.casefold() in line.lower():
                    df.loc[i,'out_type']='hit wicket'
                elif 'Stumped!!'.casefold() in line.lower():
                    df.loc[i,'out_type']='stumped'
                else:
                    pass                
# want some changes
    def find_match_no():
        match_no=file_name.split('-')[3]
        if match_no=='qualifier':
            match_no=file_name.split('-')[3] +" " + file_name.split('-')[4]
        df['match_no']=match_no


    def get_moth():
        
        cricbuzz_highlights_url=url.replace('cricket-full-commentary','cricket-scores')
        print(cricbuzz_highlights_url)
        page=requests.get(cricbuzz_highlights_url)
        soup=BeautifulSoup(page.content,"html.parser")
        try: 
            win=soup.find('div',{'class':'cb-col cb-col-100 cb-min-stts cb-text-complete'}).text
            #cb-col cb-col-100 cb-mini-col cb-min-comp ng-scope
            #cb-link-undrln ng-binding
            a=soup.find('span',{'class':'cb-text-gray cb-font-12'})
            try:
                man_of_the_match=a.next_sibling.next_sibling.next_sibling.text
            except:
                man_of_the_match=None
        except:
            win = None
            man_of_the_match=None
        
        df['win']=win
        df['man_of_the_match']=man_of_the_match

    find_toss()
    find_innings()
    find_bowling_team() 
    find_batsman_bowler()
    find_runs()
    find_length()
    find_line()
    find_destination()
    find_shots()
    find_speed()
    find_out_type()
    find_match_no()
    get_moth()
    
    df.to_csv(f"Transformed_files/cleaned_{file_name}.csv",index=False)
    

def Transform_multiple_files(url,**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['ds']
    # Retrieve the result from XCom
    match_url = ti.xcom_pull(task_ids='Get_schedule', key=execution_date)

    print(match_url)
    print(type(match_url))

    

    if len(match_url)>1:
        print("working")
        for link in match_url :
            print(f'----------------{link}started------------------------')
            clean(link)
            print(f'-----------------------{link}ended---------------------------')
    elif len(match_url) == 1:
        print("1-link",match_url[0])

        clean(match_url[0])
    
    else :
        print("errrrrrrorrrrrrrrrrrrrrrrrrrr.......................")
