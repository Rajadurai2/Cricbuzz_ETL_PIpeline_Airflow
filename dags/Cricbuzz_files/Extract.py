
def web_scrap(url):
    #url = "https://www.cricbuzz.com/cricket-full-commentary/75437/ind-vs-aus-5th-match-icc-cricket-world-cup-2023"
    # ti = kwargs['ti']
    # execution_date = kwargs['ds']
    # # Retrieve the result from XCom
    # match_url = ti.xcom_pull(task_ids='branch_task', key=execution_date)
    # print(match_url)
    print("Welcome")
    import pandas as pd
    from selenium import webdriver
    from bs4 import BeautifulSoup
    import pandas as pd
    import requests
    import time
    from selenium.webdriver.common.by import By
    from selenium import webdriver
    from Cricbuzz_files.config import team_names_dict


    ##Options for chromedriver, when using selenium, does not matter when using Collab since it acts more like a remote machine

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    
    #driver = webdriver.Chrome(service=service, options=chrome_options)
    # "http://localhost:4444/wd/hub" 
    # docker container  "http://chrome:4444/wd/hub"
    selenium_url =  "http://chrome:4444/wd/hub"

    driver = webdriver.Remote(command_executor=selenium_url, options=chrome_options)
    
    print("driver created succesfully")
    cricbuzz_highlights_url=url
    #Navigate the WebDriver instance (driver) to a specific URL
    driver.get(cricbuzz_highlights_url)
    print("page loaded")
    # load the driver contents to beatifulSoup to extract data
    cricbuzz_soup= BeautifulSoup(driver.page_source, 'html.parser')

    div_tag=cricbuzz_soup.find_all('div',{'class':"cb-col cb-col-100 ng-scope"})
    
    # get the match name from url given

    match_name=cricbuzz_highlights_url.split('/')[-1]
    #match_name

    # get the Series,Year,Statdium,Date of the match

    match_metadata_values=[c.next_element.next_element.next_element.text.replace('\xa0','') for c in cricbuzz_soup.find_all('span',{'class':'text-bold'})]

    # split the meta values 

    year=match_metadata_values[0].split()[-1]
    series=match_metadata_values[0].split(',')[-1]
    venue=match_metadata_values[1]
    date=match_metadata_values[2]+year

    # find the match no 

    def match_no():
        team_names=cricbuzz_soup.find('div',{'class':"cb-billing-plans-text cb-team-lft-item"})
        teams=team_names.text.split(',')[1]
        return teams.strip()[0:3]
    
    # convert the full name of the teams to short name 

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
        toss=cricbuzz_soup.find_all('p',{'class':"cb-com-ln ng-binding ng-scope cb-col cb-col-90"})
        for paragraph in toss:
            bold_elements = paragraph.find_all('b')
        
        # Check each <b> tag to see if it contains the text "won the toss"
            for bold in bold_elements:
                if "won the toss" in bold.get_text().lower():
                    return paragraph.get_text()
    
    def team_names():
        team_names=cricbuzz_soup.find('div',{'class':"cb-billing-plans-text cb-team-lft-item"})
        teams=team_names.text.split(',')[0]
        teams=teams.split('vs')
        teams=[team.strip() for team in teams]
        return teams
    
    # get the match details

    def match_info():
        match_details=cricbuzz_soup.find('div',{'class':"cb-col cb-col-20"})
        match_info=""
        for i in match_details:
            match_info+=i.get_text()
        return match_info
        
        # find the substitutes players of the teams


    def find_team_subs(team):
        preview_obj=cricbuzz_soup.find_all('p',{'class':'cb-com-ln ng-binding ng-scope cb-col cb-col-90'})
        for i in preview_obj:
            #print(i.text)
            if 'subs' in i.text.lower() and (team in i.text or team_names_dict[team] in i.text):
                return i.get_text()

    #find the playing 11 of each teams

    def find_team_playing11(team):
        preview_obj=cricbuzz_soup.find_all('p',{'class':'cb-com-ln ng-binding ng-scope cb-col cb-col-90'})
        for i in preview_obj:
            #print(i.text)

            if '(Playing XI)'.casefold() in i.text.casefold() and (team in i.text or team_names_dict[team] in i.text):
                return i.get_text()
    
    # get the player of the match

    def get_moth(url):
        cricbuzz_highlights_url=url
        cricbuzz_highlights_url=cricbuzz_highlights_url.replace('cricket-full-commentary','cricket-scores')
        cricbuzz_highlights_url
        page=requests.get(cricbuzz_highlights_url)
        soup=BeautifulSoup(page.content,'html.parser')
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
        return win,man_of_the_match

    winner,playe_of_the_match=get_moth(url=url)


    # get the Inns played in that match

    link_text=[]
    div_tag=cricbuzz_soup.find_all('div',{"class":"cb-hig-pil ng-scope"})
    for a_tag in div_tag:
        link_text.append(a_tag.a.string)
    link_text=link_text[1:]
    

    # scrap the bal by ball commentry

    cricbuzz_page_soup=[]
    ##Iterate through the Inns
    for l in link_text:
            ##Click the innings text
            
            try:
                loadMoreButton=driver.find_element(By.LINK_TEXT,l)
                loadMoreButton.click()
                time.sleep(5)  #must
                ##Collect the soup of the existing view that has been clicked by the driver element
                cricbuzz_soup_inner=BeautifulSoup(driver.page_source, 'html.parser')
                ##Append each of the page contents to a list
                cricbuzz_page_soup.append(cricbuzz_soup_inner)
                print(l,' Analysed')
                
            except Exception as e:
                print(e)
                #driver.quit()

    ##Scraping has been completed
    print ("Full scraping of key events complete...")
    ##Stop the driver element
    driver.quit()

    match_commentary_df=pd.DataFrame()
    for inn_num,cricbuzz_highlights_soup in enumerate(cricbuzz_page_soup):
        cricbuzz_innings_soup=cricbuzz_highlights_soup.find_all('div',{'class':'cb-col cb-col-8 text-bold ng-scope'})
        ball_overs=[]
        ball_commentary=[]
        for cinn_soup in cricbuzz_innings_soup:
            ball_overs.append(cinn_soup.text)
            ball_commentary.append(str(cinn_soup.find_next('p').text))
            #ball_commentry.append()
        innings_commentary_df=pd.DataFrame({'ball':ball_overs,'Commentary Text':ball_commentary})
        innings_commentary_df['ball']=innings_commentary_df['ball']
        innings_commentary_df['innings']=link_text[inn_num]
        match_commentary_df=pd.concat([match_commentary_df,innings_commentary_df])

    match_commentary_df.reset_index(inplace=True,drop=True)
    #match_commentary_df

    toss_string=find_toss()
    print("toss_string------------------",toss_string)
    team_a,team_b=team_names()

    match_commentary_df['match_no']=match_no()
    match_commentary_df['team_a']=team_a
    match_commentary_df['team_b']=team_b
    match_commentary_df['team_a_11']=find_team_playing11(team_a)
    match_commentary_df['team_b_11']=find_team_playing11(team_b)
    # team subs given only for 2023
    match_commentary_df['team_a_subs']=find_team_subs(team_a)
    match_commentary_df['team_b_subs']=find_team_subs(team_b)
    match_commentary_df['series']=series
    match_commentary_df['season']=year
    match_commentary_df['venue']=venue
    match_commentary_df['date']=date
    match_commentary_df['toss']=find_toss()
    match_commentary_df['winner']=winner
    match_commentary_df['player_of_the_match']=playe_of_the_match
    match_commentary_df['toss_winner']= team_names_dict[toss_string[:toss_string.find('have')].strip()]
    match_commentary_df['toss_choosen']= toss_string.split(' ')[-1]

    #print(match_commentary_df)
    match_commentary_df.to_csv(f"Scraped__raw_files/{match_name}.csv",index=False)



def Scrap_multiple_sites(**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['ds']
    # Retrieve the result from XCom
    match_url = ti.xcom_pull(task_ids='Get_schedule', key=execution_date)

    print(match_url)
    print(type(match_url))

    url = match_url

    if len(url)>1:
        print("working")
        for link in url :
            print(f'{link}started.......................................')
            web_scrap(link)
            print(f'{link}ended ..........................................')
    elif len(url) == 1:
        print("1-link",url[0])

        web_scrap(url[0])
    
    else :
        print("errrrrrrorrrrrrrrrrrrrrrrrrrr.......................")






    


