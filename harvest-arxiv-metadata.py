#!/usr/bin/python

import os.path
import time
import urllib2
import datetime
import xml.etree.ElementTree as ET
import sqlite3 as sql

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
                    abstract text, categories text, created text,
                    doi text, comments text)''')
    c.execute('''CREATE TABLE authors
                   (first_name text, last_name text, affiliation text,
                    paper_id text)''')
    c.execute('''CREATE UNIQUE INDEX id on papers (id)''')
    c.execute('''CREATE INDEX datestamp on papers (datestamp)''')
    c.execute('''CREATE INDEX paper_id on authors (paper_id)''')
    c.execute('''CREATE INDEX author_name on authors (last_name, first_name)''')
    conn.commit()
    start_date = earliest_datestamp
else:
    print "Searching for latest datestamp..."
    c.execute("SELECT MAX(datestamp) FROM papers")
    start_date = c.fetchone()[0]
    # Increase by one day to avoid re-downloading the papers from the last day
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    start_date = start_date + datetime.timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%d")
    
print "Start date: %s" % start_date

def get_text(found):
    if found is None:
        return ''
    else:
        return found.text

def add_record(c, record):
    header = record.find(OAI+'header')
    meta = record.find(OAI+'metadata')
    info = meta.find(ARXIV+"arXiv")

    oai_id = header.find(OAI+'identifier').text
    title = get_text(info.find(ARXIV+"title"))
    datestamp = get_text(header.find(OAI+'datestamp'))
    paper_id = info.find(ARXIV+"id").text
    abstract = get_text(info.find(ARXIV+"abstract")).strip()

    print "adding: %s '%s'" % (datestamp, title)

    created = get_text(info.find(ARXIV+"created"))
    #created = datetime.datetime.strptime(created, "%Y-%m-%d")
    categories = get_text(info.find(ARXIV+"categories"))

    # if there is more than one DOI use the first one
    # often the second one (if it exists at all) refers
    # to an eratum or similar
    doi = get_text(info.find(ARXIV+"doi"))
    if doi != '':
        doi = doi.split()[0]

    comments = get_text(info.find(ARXIV+"comments"))

    # (id text, oai_id text, title text, datestamp text,
    #  abstract text, categories text, created text,
    #  doi text, comments text))

    c.execute('''INSERT INTO papers VALUES (?,?,?,?,?,?,?,?,?)''',
              (paper_id, oai_id, title, datestamp, abstract, categories,
               created, doi, comments))

    authors = info.find(ARXIV+"authors")
    if authors is not None:
        for author in authors.findall(ARXIV+'author'):
            last_name = get_text(author.find(ARXIV+'keyname'))
            first_name = get_text(author.find(ARXIV+'forenames'))
            affiliation = get_text(author.find(ARXIV+'affiliation'))

            #print "author-params: ", (first_name, last_name, affiliation, paper_id)

            c.execute('''INSERT INTO authors VALUES (?,?,?,?)''',
                      (first_name, last_name, affiliation, paper_id))
            

def harvest(conn, c, arxiv_set):
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    url = (base_url +
           "from=%s&metadataPrefix=arXiv&set=%s" % (start_date, arxiv_set))
    
    while True:
        print "\n>>> fetching %s\n" % url
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

        all_records = root.find(OAI+'ListRecords')

        if all_records is None:
            break
            
        for record in all_records.findall(OAI+"record"):
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
