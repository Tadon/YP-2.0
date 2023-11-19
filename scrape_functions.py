import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import re


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
    def get_business_name(business_profile_soup):
        try:
            business_name_element = business_profile_soup.find('div', class_= 'sales-info')
            if business_name_element:
                business_name = business_name_element.find('h1', class_="dockable business-name")
                return business_name.get_text(strip=True)
                
            return '_No Name Found_'
        except Exception as e:
            return f'_Exception: {str(e)}_'
    @staticmethod
    def get_business_phone(business_profile_soup):
        try:
            business_phone_element = business_profile_soup.find('a', class_= 'phone dockable')
            if business_phone_element:
                phone_number = business_phone_element['href']
                phone_number = phone_number.replace('tel:', '').translate({ord(c): None for c in '()- '})
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
