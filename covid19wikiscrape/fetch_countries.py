# A script to scrape national coronavirus pandemic data from wikipedia.
# This script simply fetches all the available tables. It makes no effort
# to clean or filter the data. 

# This script starts with the Wikipedia table of countries 
# and territories with coronavirus pandemic data provided at 
# https://en.m.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data#covid19-container
# For each wikipedia URL in that list, it fetches the wikipedia page 
# and extracts all tables off of that page. The output file hierarchy is stored
# in files as output-<datetimestamp>/<country>/country<num>.csv.

import ray
from bs4 import BeautifulSoup
from os import path
import sys
import re
#from scrape_tables import scrape_tables_from_url
import requests
from requests.exceptions import Timeout
from bs4 import BeautifulSoup
import os
from slugify import slugify
import ray


# poolargs passes in pairs of [row, [outdir, prefix, timeout, verbose]]
@ray.remote
def fetch_countries(tr, args):

    def _add_unique_postfix(fn):
        # __author__ = 'Denis Barmenkov <denis.barmenkov@gmail.com>'
        # __source__ = 'http://code.activestate.com/recipes/577200-make-unique-file-name/'
        if not os.path.exists(fn):
            return fn

        path, name = os.path.split(fn)
        name, ext = os.path.splitext(name)

        make_fn = lambda i: os.path.join(path, '%s(%d)%s' % (name, i, ext))

        for i in xrange(2, sys.maxint):
            uni_fn = make_fn(i)
            if not os.path.exists(uni_fn):
                return uni_fn

        return None


    # create a file in output_dir for each table in the HTML at a given url
    def scrape_tables_from_url(url, output_dir, file_prefix, timeout=3, verbose=False):
        prefix = slugify(file_prefix, replacements=[['%','_percent_'],[':','_colon_']])
        errors = []
        try:
            if verbose:
                cprint("fetching " + url, 'green') 
            html = requests.get(url, timeout=timeout)
            html = html.text
            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table')
            print(prefix + ": " +str(len(tables)) + " tables")
            seqno = 1
            for t in tables:
                subdir = output_dir + '/' + prefix
                try:
                    os.mkdir(subdir)
                    if verbose:
                        cprint("created directory " + subdir, 'cyan')
                except FileExistsError:
                    pass
                filename = output_dir + '/' + prefix + '/' + prefix + str(seqno) + ".csv"
                out = open(_add_unique_postfix(filename), "w")
                if verbose:
                    cprint("created file " + filename, 'cyan')
                caption = t.find('caption')
                if caption and len(caption) > 0:
                    print("'caption'", end = ",", file = out)
                    print("\'" + str(caption) + "\'", file = out)
                rows = t.find_all('tr')
                for r in rows:
                    # break when we see class="sortbottom" in a <tr> tag
                    if r.has_attr('class') and 'sortbottom' in r['class']:
                        break
                    headers = r.find_all(['th'])
                    thtd = r.find_all(['th','td'])
                    txt = str([i.text.rstrip() for i in thtd])
                    print(txt.lstrip('[').rstrip(']'), file = out)
                seqno += 1
        except Timeout:
            cprint('----> http fetch timeout for ' + prefix, 'red')
            return(1)
        return(0)

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
