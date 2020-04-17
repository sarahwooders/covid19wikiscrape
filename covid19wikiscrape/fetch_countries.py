# A script to scrape national coronavirus pandemic data from wikipedia.
# This script simply fetches all the available tables. It makes no effort
# to clean or filter the data. 

# This script starts with the Wikipedia table of countries 
# and territories with coronavirus pandemic data provided at 
# https://en.m.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data#covid19-container
# For each wikipedia URL in that list, it fetches the wikipedia page 
# and extracts all tables off of that page. The output file hierarchy is stored
# in files as output-<datetimestamp>/<country>/country<num>.csv.

from bs4 import BeautifulSoup
from os import path
import sys
import re
from scrape_tables import scrape_tables_from_url

# poolargs passes in pairs of [row, [outdir, prefix, timeout, verbose]]
def fetch_countries(tr, args):
    outdir, prefix, timeout, verbose = args
    tr = BeautifulSoup(tr, 'lxml')
    urls = tr.find_all('a', {'href': re.compile(r'/wiki/')})
    for url in urls:
        u = prefix + url['href']
        country = url['href'].lstrip('/wiki/')
        country = re.split(r'(_in_|_on_)', country)[-1]
        if scrape_tables_from_url(u, outdir, country, timeout, verbose):
            return(u)
    return(None) # no error