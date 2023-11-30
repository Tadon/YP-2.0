# YP-2.0

New scraper for yellowpages.
search_information.py omitted to prevent "plug and play". Not intended to be used, just a fun project.
Implemented scraping to use PostgreSQL database instead of CSV to allow for easier data manipulation.
More refractorization and notes to make program easier to read/understand.
Using new modules psycopg2, concurrent.future(ThreadPoolExecutor), threading(Lock). hashlib, re, and urllib3.
Using hashing function to create unique identifier to ensure only unique businesses added to database, instead of just phone number.
Added multi-threading capabilities to speed up scraping.
