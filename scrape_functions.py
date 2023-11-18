import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


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
    def get_email(business_profile_soup):
        try:
            info_block = business_profile_soup.find('a', class_='email-business')
            if info_block:
                email_raw = info_block['href']
                email = email_raw.replace('mailto:', '')
                return email
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
            