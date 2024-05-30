import sys
def get_schedule(url):
    import json
    from bs4 import BeautifulSoup
    from datetime import datetime
    from selenium import webdriver

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    selenium_url =  "http://chrome:4444/wd/hub"


    ipl = url
    print("welcome")
    driver = webdriver.Remote(command_executor=selenium_url, options=chrome_options)
    print("driver created")
    driver.get(ipl)
    print("page loaded")

    cricbuzz_soup= BeautifulSoup(driver.page_source, 'html.parser')

    driver.quit()
    date=cricbuzz_soup.find_all('div',{'class':['cb-col-100 cb-col cb-series-matches ng-scope','cb-col-100 cb-col cb-series-brdr cb-series-matches ng-scope']})
    match_links = {}
    new_cricket_scores={}
    prev_key = None
    for i in date:
        day = (i.find('div',{'class':'cb-col-25 cb-col pad10 schedule-date ng-isolate-scope'})).text
        try:
            link = i.a['href']
        except:
            link = ""
        a = link.split('/')[2:]
        a = '/'.join(a)
        http = 'https://www.cricbuzz.com/cricket-full-commentary/'
        link = http+a
        
        if day == '\xa0' :
            match_links[prev_key] += [link]
        
        else:
            new_cricket_scores[day] = [link]
            prev_key = day

        
        match_links[day] = [link]
    keys_to_remove = [key for key in match_links if not key.isascii()]

    for key in keys_to_remove:
        del match_links[key]
            
    year = ipl.split("/")[-2].split("-")[-1] 
    dates = match_links.items()
    new_dict={}
    for key,value in dates:
        i = key.split(',')[0]
        i =  f"{i} {year}"
        date_with_year = datetime.strptime(i,"%b %d %Y")
        formatted_date = date_with_year.strftime('%Y-%m-%d')
        new_dict[formatted_date]=value
        
    
    with open('schedule.json', 'w') as json_file:
        json.dump(new_dict, json_file, indent=2)
    print("file created")


if __name__ == '__main__':
    print(sys.argv[1])
    get_schedule(sys.argv[1])
