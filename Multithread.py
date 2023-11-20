import psycopg2
from scrape_functions import ScrapeFunctions
from search_information import SearchInformation
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

#taking top 200 cities of each state and putting them in new Dictionary, for example; "City #1" : "Los Angeles", "Atlanta", "Seattle", etc..
#so that we're scraping the most populated city of each state first, instead of the top 200 cities in each state before moving to next state
iterating_search_dictionary = {}
for i in range(200):
    for state, cities in SearchInformation.city_and_states_full.items():
        if i < len(cities):
            key = f'City # {i+1}'
            if key not in iterating_search_dictionary:
                iterating_search_dictionary[key] = []
            iterating_search_dictionary[key].append(cities[i])
#creating retry function to re-establish connection if we get http error
session = ScrapeFunctions.requests_retry_session()
#initialize lock for thread safe operations
lock = Lock()
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

with ThreadPoolExecutor(max_workers = 2) as executor:
    for city_num, cities in iterating_search_dictionary.items():
        for city in cities:
            for category in SearchInformation.category_list:
                executor.submit(ScrapeFunctions.process_city_category, city, category,db_params, phone_carrier_dict,existing_numbers,lock)

conn.close()
print('Scraping complete!')
