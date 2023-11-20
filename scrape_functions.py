import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import re
import hashlib
import requests
from bs4 import BeautifulSoup
import psycopg2.extras
import time
class ScrapeFunctions:
    @staticmethod
    def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500,502,504), session=None):
        session = session or requests.Session()
        retry = Retry(
            total = retries,
            read = retries,
            connect = retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            )  
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session  
    @staticmethod
    def get_business_name(search_results_page):
        try:
            business_name_element = search_results_page.find('a', class_= 'business-name')
            if business_name_element:
                business_name = business_name_element.text
                return business_name
                
            return '_No Name Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_business_phone(search_page):
        try:
            business_phone_element = search_page.find('div', class_= 'phones phone primary')

            if business_phone_element is None:
                business_phone_element = search_page.find('div', class_= 'phone')
            if business_phone_element:
                phone_number = business_phone_element.get_text(strip=True)
                phone_number = phone_number.translate({ord(c): None for c in '()- '})
                phone_number = ''.join(filter(str.isdigit, phone_number))
                return phone_number
            return '_No Phone Provided_'
        except Exception as e:
            return f'_Exception: {str(e)}'
    @staticmethod
    def get_emails(business_profile_soup):
        try:
            info_block = business_profile_soup.find_all('a', class_='email-business')
            if info_block:
                email_list = []
                for block in info_block:
                    title = block.text.strip()
                    email_raw = block['href']
                    email = email_raw.replace('mailto:', '')
                    email_list.append(f'{title}, {email}')
                return email_list
            return '_No Email Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_address(block):
        try:
            address_element = block.find('div', class_='street-address')
            if address_element:
                street_address = address_element.text
                return street_address
            return 'No Address Found'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_locality(block):
        try:
            locality_element = block.find('div', class_= 'locality')
            if not locality_element:
                locality_element = block.find('p', class_='adr')
            if locality_element:
                locality = locality_element.text
                return locality
            
            return 'Locality Not Found'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def full_address(street_address, business_locality):
            try:
                if street_address != 'No Address Found':
                    full_address = f'{street_address} {business_locality}'
                    return full_address
                if street_address == 'No Address Found':
                    full_address = business_locality
                    return full_address
                return 'Address not found'
            except Exception as e:
                return f'Exception: {str(e)}'
    @staticmethod
    def get_website(business_profile_soup):
        try:
            website_block = business_profile_soup.find('a', class_= 'website-link dockable')
            if website_block:
                website = website_block['href']
                return website
            return 'No Website Found'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def get_services(business_profile_soup):
        try:
            service_block = business_profile_soup.find('dd', class_="features-services")
            services = []
            if service_block:
                span_elements = service_block.find_all('span')
                for span in span_elements:
                    services.append(f'{span.text.strip()} ')
                return services
            return 'No Service Found'
        except Exception as e:
            return f'Exception {str(e)}'
    @staticmethod
    def get_years_in_business(business_profile_soup):
        years_in_business_block = business_profile_soup.find('div', class_='years-in-business')
        try:    
            if years_in_business_block:
                count_block = years_in_business_block.find('div', class_= 'count')
                if count_block:
                    years_in_business = count_block.text
                    return years_in_business
            return 'No Years In Business Provided'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def get_other_links(business_profile_soup):
        other_link_block = business_profile_soup.find_all('a', class_= 'other-links')
        try:    
            if other_link_block:
                other_links = []
                for block in other_link_block:
                    link = block['href']
                    other_links.append(f'{link} ')
                return other_links
            return 'No Other Links Provided'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def get_social_links(business_profile_soup):
        social_link_block = business_profile_soup.find_all('a', class_= 'general-social-links')
        try:    
            if social_link_block:
                social_links = []
                for link in social_link_block:
                    social_link = link['href']
                    social_links.append(f'{social_link} ')
                return social_links
            return 'No Social Links Provided'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def get_categories(business_profile_soup):
        category_block = business_profile_soup.find('div', class_= 'categories')
        try:
            if category_block:
                categories = []
                category_container = category_block.find_all('a')
                for category in category_container:
                    categories.append(category.text)
                return categories
            return 'No Categories Provided'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def get_also_known_as(business_profile_soup):
        try:
            aka_block = business_profile_soup.find('dd', class_= 'aka')
            if aka_block:
                aka = aka_block.text
                return aka
            return 'No \'A.K.A\' Found'
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def get_extra_phones(business_profile_soup):
        try:
            business_phone_element = business_profile_soup.find('dd', class_='extra-phones')
            if business_phone_element:
                phone_spans = business_phone_element.find_all('span')
                for span in phone_spans:
                    
                    match = re.search(r'\(\d{3}\)\s*\d{3}-\d{4}', span.text)
                    if match:
                        phone_number = match.group()
                        phone_number = phone_number.translate({ord(c): None for c in '()- '})
                        phone_number = ''.join(filter(str.isdigit, phone_number))
                        return phone_number
                return '_No Phone Provided_'
        except Exception as e:
            return f'_Exception: {str(e)}'
    @staticmethod
    def get_general_info(business_profile_soup):
        try:
            gen_info_block = business_profile_soup.find('dd', class_='general-info')
            if gen_info_block:
                general_info = gen_info_block.text
                return general_info
            return 'No General Info Provided'
        except Exception as e:
            return f'Exception {str(e)}'
    @staticmethod
    def unique_identifier(business_phone, business_name):
        try:
            data_string = f'{business_name}{business_phone}'
            hash_object = hashlib.md5(data_string.encode())
            return hash_object.hexdigest()
        except Exception as e:
            return f'Exception: {str(e)}'
    @staticmethod
    def process_city_category(city, category, db_params, phone_carrier_dict, existing_numbers, lock):
        print('thread started')
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        counter = 1
        total_counter = 0
        success = False
        batch_data = []
        session = ScrapeFunctions.requests_retry_session()
        while True:
            url = f'https://www.yellowpages.com/search?search_terms={category}&geo_location_terms={city}&page={counter}'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0'
            }
            
            for attempt in range(1000):
                try:
                    r = session.get(url, headers=headers, timeout=10)
                    success = True
                    break
                except requests.exceptions.RequestException as e:
                    if attempt >= 999:
                        print(f'Max attempts reached, Error: {str(e)}')
                        success = False
                        break
                    time.sleep(5)
            
            if not success:
                break

            search_results_page = BeautifulSoup(r.content, 'html.parser')
            data_blocks = search_results_page.find_all('div', class_= 'v-card')
            
            business_counter = 0
            for block in data_blocks:
                business_phone = ScrapeFunctions.get_business_phone(block)
                business_name = ScrapeFunctions.get_business_name(block)
                unique_id = ScrapeFunctions.unique_identifier(business_name, business_phone)
                
                if unique_id not in existing_numbers:
                    anchor = block.find('a', class_= 'business-name')
                    
                    if anchor:
                        
                        html = f'https://yellowpages.com{anchor["href"]}' if anchor else '_No Hyperlink Found_'
                        
                        if html != '_No Hyperlink Found_' and html != 'https://yellowpages.com':
                            new_request = requests.get(html, headers = headers)
                            business_profile_soup = BeautifulSoup(new_request.content, 'html.parser')
                            business_email = ScrapeFunctions.get_emails(business_profile_soup)
                            street_address = ScrapeFunctions.get_address(block)
                            business_locality = ScrapeFunctions.get_locality(block)
                            full_address = ScrapeFunctions.full_address(street_address, business_locality)
                            business_website = ScrapeFunctions.get_website(business_profile_soup)
                            business_services = ScrapeFunctions.get_services(business_profile_soup)
                            years_in_business = ScrapeFunctions.get_years_in_business(business_profile_soup)
                            other_links = ScrapeFunctions.get_other_links(business_profile_soup)
                            social_links = ScrapeFunctions.get_social_links(business_profile_soup)
                            company_categories = ScrapeFunctions.get_categories(business_profile_soup)
                            also_known_as = ScrapeFunctions.get_also_known_as(business_profile_soup)
                            extra_phones = ScrapeFunctions.get_extra_phones(business_profile_soup)
                            general_info = ScrapeFunctions.get_general_info(business_profile_soup)
                            area_exchange_code = business_phone[:6]
                            phone_carrier = phone_carrier_dict.get(area_exchange_code, '_No Carrier Found_')
                            business_data = (business_name, business_phone, business_email,full_address, business_website, business_services, years_in_business, other_links, social_links, company_categories, also_known_as, extra_phones,phone_carrier, general_info, unique_id)
                            business_counter += 1
                            print(f'Scraped business #{business_counter} from page {counter} of {category} in {city}')
                            with lock:                                
                                if unique_id not in existing_numbers:
                                    existing_numbers[unique_id] = ''
                                    
                                    batch_data.append(business_data)
                                    
                                    
            total_counter += business_counter 
            print(f'{business_counter} entries being batched to database. Total count for this thread is {total_counter}.')               
            query = ''' INSERT INTO yp_2 ("Business Name","Business Phone","Business Emails","Business Address","Business Website","Business Services","Years in Business","Other Links","Social Links","Company Categories","Company AKA","Extra Phones","Phone Carrier","General Information", "Unique ID")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            if batch_data:
                psycopg2.extras.execute_batch(cur, query, batch_data)
                conn.commit()
                batch_data.clear()
            
            next_page = search_results_page.find('a', class_= 'next ajax-page')
            if next_page:
                counter += 1
            else:
                break
        cur.close()
        conn.close()

