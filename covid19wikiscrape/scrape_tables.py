import requests
from requests.exceptions import Timeout
from bs4 import BeautifulSoup
import os
from slugify import slugify

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
            print("'caption'", end = ",", file = out)
            print("\'" + str(t.find('caption')) + "\'", file = out)
            rows = t.find_all('tr')
            for r in rows:
                # break when we see class="sortbottom" in a <tr> tag
                if r.has_attr('class') and 'sortbottom' in r['class']:
                    break
                thtd = r.find_all(['th','td'])
                txt = str([i.text.rstrip() for i in thtd])
                print(txt.lstrip('[').rstrip(']'), file = out)
            seqno += 1
    except Timeout:
        cprint('----> http fetch timeout for ' + prefix, 'red')
        return(1)
    return(0)