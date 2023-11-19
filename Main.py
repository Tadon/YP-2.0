import psycopg2
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from scrape_functions import ScrapeFunctions
import time
from search_information import SearchInformation
#creating city and state array using search_information for more effecient, faster data accumulation
iterating_search_dictionary = {}
#taking top 200 cities of each state and putting them in new Dictionary, for example; "City #1" : "Los Angeles", "Atlanta", "Seattle", etc..
#so that we're scraping the most populated city of each state first, instead of the top 200 cities in each state before moving to next state
for i in range(200):
    for state, cities in SearchInformation.city_and_states_full.items():
        if i < len(cities):
            key = f'City # {i+1}'
            if key not in iterating_search_dictionary:
                iterating_search_dictionary[key] = []
            iterating_search_dictionary[key].append(cities[i])
#creating retry function to re-establish connection if we get http error
session = ScrapeFunctions.requests_retry_session()

#DB Connection parameters
db_params = SearchInformation.db_params
 #connecting to database

conn = psycopg2.connect(**db_params)

#Adding phone numbers from exisiting database as unique keys to ensure no duplicate phone numbers
cur = conn.cursor()
#executing select action to SQL database
cur.execute('SELECT "Business Phone" FROM yp_2')
#adding phone numbers from business phones in db to list named rows
rows = cur.fetchall()
#adding business count for keeping track of how many businesses you havei n db
business_counter = 0
#creating hashmap to verify that no new duplicated numbers will be added to db
existing_numbers = {}
#adding each phone number in row list from db as key to existing numbers hashmap to make unique number search more efficient
for row in rows:
    phone_number = row[0]
    existing_numbers[phone_number] = ''
    business_counter += 1

#Connecting to carrier DB to verify phone carrier
cur = conn.cursor()
cur.execute('SELECT acex, "Company" FROM carrier_db')
rows = cur.fetchall()
#adding the phone number as key and phone carrier as pair in this dictionary for future comparison reason
phone_carrier_dict = {row[0]: row[1] for row in rows}

session_counter = 0
#for key value pairs in iterating search dictionary
for city_num, cities in iterating_search_dictionary.items():
    #for the values of each key (Key = City #1, Value = Most populated cities in each state)
    for city in cities:
            #iterating through each category when searching for each city
            for category in SearchInformation.category_list:
                #page counter for url
                counter = 1
                while True:
                    #setting variables to enable BeautifulSoup
                    url = f'https://www.yellowpages.com/search?search_terms={category}&geo_location_terms={cities}&page={counter}'
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0'
                    }
                    
                    success = False

                    #connecting to website
                    for attempt in range(1000):
                        try:
                            r = session.get(url, headers = headers, timeout = 10)
                            success = True
                            
                            break
                        except requests.exceptions.RequestException as e:
                            if attempt > 999:
                                print ('Max attempts reached, moving to next.')
                                print(f'Error: {str(e)}')
                            time.sleep(5)
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
                        #Now moving to actually scraping the information
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
                            business_email = ScrapeFunctions.get_emails(business_profile_soup)
                            #address
                            street_address = ScrapeFunctions.get_address(block)
                            #locality
                            business_locality = ScrapeFunctions.get_locality(block)
                            #full address
                            full_address = ScrapeFunctions.full_address(street_address,business_locality)
                            #business website
                            business_website = ScrapeFunctions.get_website(business_profile_soup)
                            #services that business offers
                            business_services = ScrapeFunctions.get_services(business_profile_soup)
                            #years in business
                            years_in_business = ScrapeFunctions.get_years_in_business(business_profile_soup)
                            #other links
                            other_links = ScrapeFunctions.get_other_links(business_profile_soup)
                            #social links
                            social_links = ScrapeFunctions.get_social_links(business_profile_soup)
                            #categories
                            company_categories = ScrapeFunctions.get_categories(business_profile_soup)
                            #Getting AKA
                            also_known_as = ScrapeFunctions.get_also_known_as(business_profile_soup)
                            #Extra phones
                            extra_phones = ScrapeFunctions.get_extra_phones(business_profile_soup)
                            #General Info
                            general_info = ScrapeFunctions.get_general_info(business_profile_soup)
                            #formatting business phone number as code so we can compare it to carrier_db to determine which carrier owns phone number
                            area_exchange_code = business_phone[:6]
                            phone_carrier = phone_carrier_dict.get(area_exchange_code, '_No Carrier Found_')

                            #Define insert statement to push data to SQL db
                            query = '''
                            INSERT INTO yp_2 ("Business Name","Business Phone","Business Emails","Business Address","Business Website","Business Services","Years in Business","Other Links","Social Links","Company Categories","Company AKA","Extra Phones","Phone Carrier","General Information")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            '''
                            #executing instert statement
                            data = (business_name, business_phone, business_email, full_address,business_website,business_services, years_in_business, other_links, social_links, company_categories, also_known_as,extra_phones,phone_carrier,general_info)
                            with conn.cursor() as cur:
                                cur.execute(query, data)
                                conn.commit()
                            #determine if there's a next page to navigate to, then moving the page over if it does, breaking if it doesn't
                            next_page_exists = search_page.find('a', class_='next ajax-page')
                            #print statement to give visual confirmation that scraping is happening
                            print(f'Processed page {counter} of {category} in {city}, with a total of {business_counter} businesses added.')

                            if next_page_exists:
                                counter += 1
                            else:
                                break
                    break
