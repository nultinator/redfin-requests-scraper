import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def scrape_search_results(search_info, location, page_number, retries=3):
    formatted_locality = search_info["locality"].replace(" ", "-")
    url = f"https://www.redfin.com/city/{search_info['id_number']}/{search_info['state']}/{formatted_locality}/page-{page_number+1}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")
            
            script_tags = soup.select("script[type='application/ld+json']")
            for script in script_tags:
                json_data = json.loads(script.text)
                if type(json_data) != list:
                    continue

                product = {}
                for element in json_data:
                    if element["@type"] == "Product":
                        product = element
                        break

                search_data = {
                    "name": product["name"],
                    "price": product["offers"]["price"],
                    "price_currency": product["offers"]["priceCurrency"],
                    "url": product["url"]
                }
                
                print(search_data)               

            logger.info(f"Successfully parsed data from: {url}")
            success = True        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")




def start_scrape(search_info, pages, location, retries=3):
    for page in range(pages):
        scrape_search_results(search_info, location, page, retries=retries)


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    location_list = [{"id_number": 12572, "state": "SC", "locality": "Myrtle Beach"}]
    aggregate_files = []

    ## Job Processes
    for search_area in location_list:
        filename = search_area["locality"].replace(" ", "-")

        start_scrape(search_area, PAGES, LOCATION, retries=MAX_RETRIES)
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")