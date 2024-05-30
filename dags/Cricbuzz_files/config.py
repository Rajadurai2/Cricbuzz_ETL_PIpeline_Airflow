
from datetime import datetime

# change your start time here
# format = daterime(YYYY,MM,DD,HH,MM)

start_date = datetime(2024,5,17,23,59)


# Edit or your Add your team names with their short names here


team_names_dict = {
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
    'Royal Challengers Bengaluru':'RCB',
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




def short_name(team):
    return team_names_dict[team]