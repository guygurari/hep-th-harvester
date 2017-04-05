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
db_filename = "db/hep-th.sqlite"

def get_text(found):
    if found is None:
        return ''
    else:
        return found.text

def is_in_db(cursor, paper_id):
    """Is the given paper ID in the database?"""
    cursor.execute("SELECT id FROM arxiv_papers where id=?", (paper_id,))
    return cursor.fetchone() != None

def add_record(cursor, record):
    header = record.find(OAI+'header')
    meta = record.find(OAI+'metadata')
    info = meta.find(ARXIV+"arXiv")

    oai_id = header.find(OAI+'identifier').text
    title = get_text(info.find(ARXIV+"title"))
    datestamp = get_text(header.find(OAI+'datestamp'))
    paper_id = info.find(ARXIV+"id").text
    abstract = get_text(info.find(ARXIV+"abstract")).strip()

    if is_in_db(cursor, paper_id):
        print "%s : skipping (already in database)" % paper_id
        return

    print "%s : adding : %s '%s'" % (paper_id, datestamp, title)

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

    cursor.execute('''INSERT INTO arxiv_papers VALUES (?,?,?,?,?,?,?,?,?)''',
              (paper_id, oai_id, title, datestamp, abstract, categories,
               created, doi, comments))

    authors = info.find(ARXIV+"authors")
    if authors is not None:
        for author in authors.findall(ARXIV+'author'):
            last_name = get_text(author.find(ARXIV+'keyname'))
            first_name = get_text(author.find(ARXIV+'forenames'))
            affiliation = get_text(author.find(ARXIV+'affiliation'))
            cursor.execute('''INSERT INTO arxiv_authors VALUES (?,?,?,?)''',
                      (first_name, last_name, affiliation, paper_id))
            

def harvest(conn, cursor, arxiv_set):
    start_date = find_latest_start_date(cursor)
    print "Start date: %s" % start_date

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
            add_record(cursor, record)

        conn.commit()

        # The list of articles returned by the API comes in chunks of
        # 1000 articles. The presence of a resumptionToken tells us that
        # there is more to be fetched.
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            print "\nNo resumption token, we are done\n"
            break
        else:
            url = base_url + "resumptionToken=%s"%(token.text)
            
def find_latest_start_date(cursor):
    cursor.execute("SELECT MAX(datestamp) FROM arxiv_papers")
    start_date = cursor.fetchone()

    if start_date == None:
        print "Date not found, starting at earliest datestamp."
        return earliest_datestamp
    else:
        start_date = start_date[0]
        # start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        # start_date = start_date + datetime.timedelta(days=1)
        # start_date = start_date.strftime("%Y-%m-%d")
        return start_date
    
def main():
    conn = sql.connect(db_filename)
    cursor = conn.cursor()

    if not os.path.isfile(db_filename):
        print "Database file %s not found" % db_filename
        exit(1)

    harvest(conn, cursor, arxiv_set)
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    main()
