import psycopg2
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from scrape_functions import ScrapeFunctions
import time

#creating retry function to re-establish connection if we get http error
session = ScrapeFunctions.requests_retry_session()

#DB Connection parameters

try:
    while True:
        #setting variables to enable BeautifulSoup
        url = f'https://www.yellowpages.com/search?search_terms=Plumbers&geo_location_terms=Little+Rock%2C+AR'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0'
        }
        print('Connecting...')
        success = False

        #connecting to website
        for attempt in range(1000):
            try:
                r = session.get(url, headers = headers, timeout = 10)
                success = True
                print('Connected')
                break
            except requests.exceptions.RequestException as e:
                if attempt > 999:
                    print ('Max attempts reached, moving to next.')
                    print(f'Error: {str(e)}')
                time.sleep(1)
        if not success:
            continue

        #search page soup used to grab each business profile url
        search_page = BeautifulSoup(r.content, 'html.parser')
        #creating list of windows that contain url we want
        data_blocks = search_page.find_all('div', class_= 'v-card')
        #iterating through each business listing on the search screen
        for block in data_blocks:
            #finding the profile hyperlink in each block of data
            anchor = block.find('a', class_='business-name')
            html = f'https://yellowpages.com{anchor["href"]}' if anchor else '_No Hyperlink Found_'

            if html != '_No Hyperlink Found_' and html != 'https://yellowpages.com':
                #new request to business profile page
                new_request = requests.get(html, headers=headers)
                #new soup of the profile page
                business_profile_soup = BeautifulSoup(new_request.content, 'html.parser')
                #business name
                business_name = ScrapeFunctions.get_business_name(business_profile_soup)
                #business phone
                business_phone = ScrapeFunctions.get_business_phone(business_profile_soup)
                #business email
                business_email = ScrapeFunctions.get_email(business_profile_soup)
                #address
                street_address = ScrapeFunctions.get_address(block)
                #locality
                business_locality = ScrapeFunctions.get_locality(block)
                #full address
                full_address = ScrapeFunctions.full_address(street_address,business_locality)
                print(full_address)
        break

except:
    pass