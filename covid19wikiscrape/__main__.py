import ray
ray.init()


import argparse
from slugify import slugify
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import Pool
from termcolor import colored, cprint
import sys
import re
from itertools import repeat


from fetch_countries import fetch_countries
import sys
sys.path.insert(1, '/Users/wooders/Documents/repos/cloudburst')
from cloudburst.client.client import CloudburstConnection      # A script to scrape national coronavirus pandemic data from wikipedia.
# This script simply fetches all the available tables. It makes no effort
# to clean or filter the data. 

# This script starts with the Wikipedia table of countries 
# and territories with coronavirus pandemic data provided at 
# https://en.m.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data#covid19-container
# For each wikipedia URL in that list, it fetches the wikipedia page 
# and extracts all tables off of that page. The output file hierarchy is stored
# in files as output-<datetimestamp>/<country>/country<num>.csv.


def main(argv):
    # parse command line
    parser = argparse.ArgumentParser(description="scrape national wikipedia data on coronavirus",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-z', '--tgz', action="store_true", default=False,
                        help="compress result directory to a single tgz file")
    parser.add_argument('-t', '--timeout', action="store", default = 10, type=float,
                        help="http fetch timeout")
    parser.add_argument('-w', '--wikiprefix', action="store", 
                        default = "https://en.m.wikipedia.org",
                        help="URL prefix of localized wikipedia pages")
    parser.add_argument('-l', '--listurl', action="store",
                        default = "https://en.m.wikipedia.org/wiki/Template:2019%E2%80%9320_coronavirus_pandemic_data#covid19-container",
                        help="URL with list of countries/territories to fetch")
    parser.add_argument('-v', '--verbose', action="store_true", default=False,
                        help="print more info")
    parser.add_argument('-j', '--threads', action="store", default=128, type=int,
                        help="parallel download threads")
    parser.add_argument('-r', '--runner', action="store", 
                        help="how to run parallel scraping")
    args = parser.parse_args(argv)

    # create output directory
    outdir = slugify("output-" + datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)"), replacements=[['%','_percent_'],[':','-']])
    os.mkdir(outdir)

    # fetch list of country pages in parallel
    # we get the country list from the table in args.listurl
    country_list = requests.get(args.listurl).text
    country_soup = BeautifulSoup(country_list, 'lxml')
    loc_tbl = country_soup.find('table')
    rows = loc_tbl.find_all('tr')

    # end the list of rows when we see class="sortbottom" in a <tr> tag
    idx = next(i for i,tr in enumerate(rows) if tr.has_attr('class') and 'sortbottom' in tr['class'])
    rows = rows[0:idx-1]

    # in order to parallelize, need to convert soup back to strings
    rows = [str(tr) for tr in rows]


    poolargs = [outdir, args.wikiprefix, args.timeout, args.verbose]
    if args.runner == 'multiprocessing':

        pool = Pool(args.threads)
        # starmap only takes two args, so we zip the rows with the constant args into pairs
        results = pool.starmap(fetch_countries, zip(rows, repeat(poolargs)))

    elif args.runner == 'cloudburst':
        os.chdir('/Users/wooders/Documents/repos/cloudburst')

        def f(x):
            return fetch_countries(x, poolargs)

        local_cloud = CloudburstConnection('127.0.0.1', '127.0.0.1', local=True)

        # Call with function
        cloud_fetch_countries = local_cloud.register(f, 'fetch_countries')
        calls = [cloud_fetch_countries(row) for row in rows]
        results = [call.get() for call in calls]

        # Call with DAG
        local_cloud.register_dag('dag', ['fetch_countries'], [])
        results = [local_cloud.call_dag('dag', {'fetch_countries': row}) for row in rows]

    elif args.runner == 'ray':
        print("Running ray")
        futures = [fetch_countries.remote(row, poolargs) for row in rows]
        results = ray.get(futures)

    else:
        raise ValueError("No runner option")
    # remove non-errors
    errors = list(filter(None, results))


    if args.tgz:
        outfile = outdir + '.tgz'
        os.system('tar czf ' + outfile + ' ' + outdir)
        os.system('rm -rf ' + outdir)
        if args.verbose:
            cprint('created output file ' + outfile)
    if len(errors) > 0:
        cprint("errors encountered fetching these: " + str(errors), 'red')
    else:
        print("all pages fetched successfully")

if __name__ == "__main__":
    main(sys.argv[1:])
