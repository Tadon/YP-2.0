import psycopg2
import requests
from bs4 import BeautifulSoup
from scrape_functions import ScrapeFunctions
import time
from search_information import SearchInformation
import psycopg2.extras
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
#adding business counter to keep track of total businesses in db
business_counter = 0
#executing select action to SQL database
cur = conn.cursor()
cur.execute('SELECT "Unique ID" FROM yp_2')
#taking unique ID row to make as keys for dictionary, so when we add new businesses we can run the new unique id vs the dictionary to make sure we aren't adding duplicates
rows = cur.fetchall()
existing_numbers = {}
for row in rows:
    unique_id = row[0]
    existing_numbers[unique_id] = ''
    business_counter += 1
#Connecting to carrier DB to to create another dictionary to make it easier to identify business telephone carrier.
cur = conn.cursor()
cur.execute('SELECT acex, "Company" FROM carrier_db')
rows = cur.fetchall()
phone_carrier_dict = {row[0]: row[1] for row in rows}
#counter to keep track of how many businesses are added during current session
session_counter = 0
try:
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
                        url = f'https://www.yellowpages.com/search?search_terms={category}&geo_location_terms={city}&page={counter}'
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
                        search_results_page = BeautifulSoup(r.content, 'html.parser')
                        #creating list of windows that contain url we want
                        data_blocks = search_results_page.find_all('div', class_= 'v-card')
                        #batch data list to lessen load on db connection
                        batch_data = []
                        #iterating through each business listing on the search screen
                        for block in data_blocks:
                            #scraping business phone and business name off of initial search results page, so we can create a unique identifier out of their concatenation
                            business_phone = ScrapeFunctions.get_business_phone(block)
                            business_name = ScrapeFunctions.get_business_name(block) 
                            unique_id = ScrapeFunctions.unique_identifier(business_name, business_phone)

                            if unique_id not in existing_numbers:
                                #finding the profile hyperlink in each block of data
                                anchor = block.find('a', class_='business-name')
                                if anchor:
                                    html = f'https://yellowpages.com{anchor["href"]}' if anchor else '_No Hyperlink Found_'
                                    #Now moving to actually scraping the information
                                    if html != '_No Hyperlink Found_' and html != 'https://yellowpages.com':
                                    
                                        #new request to business profile page
                                        new_request = requests.get(html, headers=headers)
                                        #new soup of the profile page
                                        business_profile_soup = BeautifulSoup(new_request.content, 'html.parser')                    
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
                                        #creating data list to append to batch data list
                                        business_data = (business_name, business_phone, business_email,full_address, business_website, business_services, years_in_business, other_links, social_links, company_categories, also_known_as, extra_phones,phone_carrier, general_info, unique_id)
                                        batch_data.append(business_data)

                                        
                                        
                                        
                                        session_counter += 1
                                        business_counter += 1
                                        existing_numbers[unique_id] = ''
                                        print(f'business {business_counter} on page {counter}')
                        #defining insert statement for batch inserts, then inserting batch_data list directly into database
                        query = ''' INSERT INTO yp_2 ("Business Name","Business Phone","Business Emails","Business Address","Business Website","Business Services","Years in Business","Other Links","Social Links","Company Categories","Company AKA","Extra Phones","Phone Carrier","General Information", "Unique ID")
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                '''        
                        with conn.cursor() as cur:
                            psycopg2.extras.execute_batch(cur, query, batch_data)
                            conn.commit()
                        batch_data.clear()        
                                #print statement to give visual confirmation that scraping is happening
                        print(f'Processed page {counter} of {category} in {city}, with a total of {business_counter} businesses added.')
                        #determine if there's a next page to navigate to, then moving the page over if it does, breaking if it doesn't
                        next_page_exists = search_results_page.find('a', class_='next ajax-page')
                        if next_page_exists:
                            counter += 1
                        else:
                            break
    #closing connecting when scraping is complete                
    conn.close()
    print('Scraping complete!')
    #if an error occurs, connecting to db terminates
finally:
    cur.close()
    conn.close()