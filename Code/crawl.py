import pickle
import time
import requests
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup
from time import sleep
from elasticsearch import Elasticsearch
from urllib.robotparser import RobotFileParser
from collections import deque


es = Elasticsearch()

seed_URLs = [
    "http://en.wikipedia.org/wiki/Cold_War",
    "http://www.historylearningsite.co.uk/coldwar.htm",
    "http://en.wikipedia.org/wiki/Cuban_Missile_Crisis",
    "https://www.jfklibrary.org/learn/education/teachers/curricular-resources/high-school-curricular-resources/the-cuban-missile-crisis-how-to-respond?gclid=Cj0KCQiAv6yCBhCLARIsABqJTjZSDc77zAgSV2TD6d90REoOnYWZ1T_6pC_iJ7UyHHvqqnQiqExnD20aAjcHEALw_wcB"
    "https://www.google.com/search?q=cuban+missile+crisis&oq=cuban+missile+crisis&aqs=chrome..69i57j0i20i263j0l8.985j0j4&sourceid=chrome&ie=UTF-8"
    "https://www.google.com/search?client=safari&rls=en&q=cuban+missile+crisis&ie=UTF-8&oe=UTF-8"
]

frontier = deque(seed_URLs) # Initialize a deque
visited = set() # Keep track of visited URLs
link_graph = {}  # Store in-links, out-links, relevance score, and wave number

def get_base_url(url):
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return base_url

#URL Canonicalization function
def canonicalize_url(base_url, URL):
    #Make relative URLs absolute
    absolute_url = urljoin(base_url, URL, allow_fragments=True)
    parsed_absolute_url = urlparse(absolute_url)
    #Convert the scheme and host to lower case
    scheme = parsed_absolute_url.scheme.lower()
    netloc = parsed_absolute_url.netloc.lower()
    #Remove port 80 from http URLs, and port 443 from HTTPS URLs
    if scheme == "http":
        netloc = netloc.replace(":80", "")
    elif scheme == "https":
        netloc = netloc.replace(":443", "")
    #Remove duplicate slashes
    path = parsed_absolute_url.path.replace('//', '/')
    #Construct a URL from a tuple and remove the fragment begins with #
    canonical_url = urlunparse((scheme, netloc, path, '', '', ''))

    return canonical_url

#make no more than one HTTP request per second from any given domain
last_req_time = {}
def rate_limit(domain):
    if domain in last_req_time:
        time_elapsed = time.time() - last_req_time[domain]
        if time_elapsed < 1:
            time.sleep(1 - time_elapsed)
    last_req_time[domain] = time.time()

#fetch the robots.txt file from a given domain
def check_crawl_availability(url, retry_count=3):
    basic_url = get_base_url(url)
    abs_url = canonicalize_url(basic_url, url)
    scheme = urlparse(abs_url).scheme 
    netloc = urlparse(abs_url).netloc

    if not netloc:  
        print(f"Malformed URL: {url}")
        return False

    robots_url = f"{scheme}://{netloc}/robots.txt"
    
    rfp = RobotFileParser()
    rfp.set_url(robots_url)
    try:
        rfp.read()
    except Exception as e:
        if retry_count > 0:
            print(f"Retry reading robots.txt for {url}")
            time.sleep((4 - retry_count) * 2)  # Exponential backoff
            return check_crawl_availability(url, retry_count - 1)
        else:
            print(f"Final error reading robots.txt for {url}: {e}")
            return False   

    return rfp.can_fetch("*", url)


#make request according to politeness policy
def make_request(url, method='GET'):
    domain = urlparse(url).netloc
    if not check_crawl_availability(url):
        print(f"Crawling is not allowed: {url}")
        return None
    
    rate_limit(domain)

    try:
        if method.upper() == 'GET':
            response = requests.get(url)
        elif method.upper() == 'HEAD':
            response = requests.head(url)
        else:
            print(f"invalid method: {method}")
            return None

        return response
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def store_elasticsearch(url, text, title):
    doc = {
        'url': url,
        'title': title,
        'text': text
    }
    es.index(index="webpages", document=doc)

def fetch_and_process(url):
    response = make_request(url)
    if not response or response.status_code != 200:
        return []
    
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    title = soup.title.string if soup.title else url
    
    text = soup.get_text(separator=' ', strip=True)
    store_elasticsearch(url, text, title)  # store the content of each page to Elasticsearch

    # Extract and canonicalize links
    links = []
    for link in soup.find_all('a', href=True):
        original_link = link['href']
        basic_url = get_base_url(original_link)
        canonical_link = canonicalize_url(basic_url, original_link)
        if canonical_link not in visited:
            links.append(canonical_link)
    
    return links

def crawl():
    while frontier and len(visited) < 30000:
        current_url = frontier.popleft()  # breath first search
        if current_url in visited:
            continue
        visited.add(current_url)

        outlinks = fetch_and_process(current_url)
        for link in outlinks:
            if link not in visited and link not in frontier:
                frontier.append(link)  # Add new links to the frontier

        if len(visited) % 500 == 0:
            with open('crawler_state.pkl', 'wb') as f:
                pickle.dump((list(frontier), list(visited)), f)

crawl() 
