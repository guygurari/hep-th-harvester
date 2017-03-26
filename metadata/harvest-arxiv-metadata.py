#!/usr/bin/python

import os.path
import time
import urllib2
import datetime
#from itertools import ifilter
#from collections import Counter, defaultdict
import xml.etree.ElementTree as ET

#from bs4 import BeautifulSoup
#import matplotlib.pylab as plt
import pandas as pd
#import numpy as np
#import bibtexparser
import sqlite3 as sql

#pd.set_option('mode.chained_assignment','warn')

OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"
arxiv_set = "physics:hep-th"
earliest_datestamp = '2007-05-23'
db_filename = "hep-th.sqlite"

is_new_database = not os.path.isfile(db_filename)
conn = sql.connect(db_filename)
c = conn.cursor()

if is_new_database:
    c.execute('''CREATE TABLE papers
                   (id text, oai_id text, title text, datestamp text,
                    abstract text, category text, created text,
                    doi text, comments text)''')
    c.execute('''CREATE TABLE authors
                   (first_name text, last_name text, affiliation text,
                    paper_id text)''')
    conn.commit()
    start_date = earliest_datestamp
else:
    c.execute("SELECT MAX(datestamp) FROM papers")
    start_date = c.fetchone()[0]
    
print "Start date: %s" % start_date

def add_record(c, record):
    header = record.find(OAI+'header')
    meta = record.find(OAI+'metadata')
    info = meta.find(ARXIV+"arXiv")

    oai_id = header.find(OAI+'identifier').text
    title = info.find(ARXIV+"title").text
    datestamp = header.find(OAI+'datestamp').text
    paper_id = info.find(ARXIV+"id").text
    abstract = info.find(ARXIV+"abstract").text.strip()

    print "datestamp: %s" % datestamp

    created = info.find(ARXIV+"created").text
    #created = datetime.datetime.strptime(created, "%Y-%m-%d")
    categories = info.find(ARXIV+"categories").text
    category = categories.split()[0] # TODO what about the others?

    # if there is more than one DOI use the first one
    # often the second one (if it exists at all) refers
    # to an eratum or similar
    doi = info.find(ARXIV+"doi")
    if doi is None:
        doi = ''
    else:
        doi = doi.text.split()[0]

    comments = info.find(ARXIV+"comments")
    if comments is not None:
        comments = comments.text
    else:
        comments = ''

    # (id text, oai_id text, title text, datestamp text,
    #  abstract text, categories text, created text,
    #  doi text, comments text))

    # print "params: ", (paper_id, oai_id, title, datestamp, abstract, category,
    #            created, doi, comments)
    c.execute('''INSERT INTO papers VALUES (?,?,?,?,?,?,?,?,?)''',
              (paper_id, oai_id, title, datestamp, abstract, category,
               created, doi, comments))

    authors = info.find(ARXIV+"authors")
    if authors is not None:
        for author in authors.findall(ARXIV+'author'):
            last_name = author.find(ARXIV+'keyname').text
            first_name = author.find(ARXIV+'forenames').text
            affiliation = author.find(ARXIV+'affiliation')
            if affiliation is None:
                affiliation = ''
            else:
                affiliation = affiliation.text

            #print "author-params: ", (first_name, last_name, affiliation, paper_id)

            #(first_name text, last_name text, affiliation text,
            # paper_id text))
            c.execute('''INSERT INTO authors VALUES (?,?,?,?)''',
                      (first_name, last_name, affiliation, paper_id))
            

def harvest(conn, c, arxiv_set):
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    url = (base_url +
           "from=%s&metadataPrefix=arXiv&set=%s" % (start_date, arxiv_set))
    
    while True:
        print "fetching", url
        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            if e.code == 503:
                to = int(e.hdrs.get("retry-after", 30))
                print "Got 503. Retrying after {0:d} seconds.".format(to)

                time.sleep(to)
                continue
            else:
                raise
            
        xml = response.read()
        root = ET.fromstring(xml)

        for record in root.find(OAI+'ListRecords').findall(OAI+"record"):
            add_record(c, record)

        conn.commit()

        # The list of articles returned by the API comes in chunks of
        # 1000 articles. The presence of a resumptionToken tells us that
        # there is more to be fetched.
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            break
        else:
            url = base_url + "resumptionToken=%s"%(token.text)
            
harvest(conn, c, arxiv_set)
conn.close()
