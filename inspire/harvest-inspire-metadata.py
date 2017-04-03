#!/usr/bin/python

import os.path
import argparse
import re
import time
import urllib2
import datetime
import xml.etree.ElementTree as ET
import sqlite3 as sql

base_harvesting_url = "http://inspirehep.net/oai2d?verb=ListRecords&"
metadata_prefix = "marcxml"
data_set = "INSPIRE:HEP"
earliest_datestamp = '1934-10-31' # from 'verb=Identify'
db_filename = "inspire.sqlite"
resumption_token_file = 'inspire-resumption-token.txt'

OAI = "{http://www.openarchives.org/OAI/2.0/}"
MARC = "{http://www.loc.gov/MARC21/slim}"

def is_in_hep_theory(info):
    for subject in info.iterfind(MARC+'datafield[@tag="650"]'):
        source = subject.findtext(MARC+'subfield[@code="2"]')
        term = subject.findtext(MARC+'subfield[@code="a"]')

        if source == 'INSPIRE' and term == 'Theory-HEP':
            return True

        if source == 'arXiv' and term == 'hep-th':
            return True

    return False

def is_in_db(cursor, inspire_id):
    """Is the given paper ID in the database?"""
    cursor.execute("SELECT id FROM inspire_papers where id=%s"
                   % inspire_id)
    return cursor.fetchone() != None

def add_author(cursor, inspire_id, elem):
    full_name = elem.findtext(MARC+'subfield[@code="a"]')

    if full_name == None:
        # Missing author name. This happens e.g. in proceedings.
        return
    
    # Sometimes there's more than one comma: "Callan, Curtis G., Jr."
    # So we don't just split the string.
    m = re.match(r"([^,]+),\s*(.*)", full_name)

    if m == None:
        last_name = full_name
        first_name = ''
    else:
        last_name = m.group(1)
        first_name = m.group(2)

    cursor.execute('''INSERT INTO inspire_authors VALUES (?,?,?)''',
              (first_name.strip(), last_name.strip(), inspire_id))
               
def add_record(cursor, record, dry_run=False):
    header = record.find(OAI+'header')
    status = header.get('status')

    if status != None and status == 'deleted':
        print "skipping (deleted record)"
        return
    
    meta = record.find(OAI+'metadata')
    info = meta.find(MARC+"record")

    inspire_id = info.findtext(MARC+'controlfield[@tag="001"]', default='')

    if is_in_db(cursor, inspire_id):
        print "%s : skipping (already in database)" % inspire_id
        return

    title = info.findtext(
        MARC+'datafield[@tag="245"]/'+
        MARC+'subfield[@code="a"]')

    if not is_in_hep_theory(info):
        #print "%s : skipping (not in hep theory)" % inspire_id
        print inspire_id
        return
    elif title == 'withdrawn or canceled':
        print "%s : skipping (withdrawn or canceled)" % inspire_id
        return

    oai_id = header.find(OAI+'identifier').text
    datestamp = header.findtext(OAI+'datestamp')

    doi = info.findtext(
        MARC+'datafield[@tag="024"]/'+
        MARC+'subfield[@code="a"]')

    created = info.findtext(
        MARC+'datafield[@tag="961"]/'+
        MARC+'subfield[@code="x"]')
    # created = datetime.datetime(
    #     *(time.strptime(created_text, '%Y%m%d%H%M%S.0')[0:6]))

    arxiv_id = None
    arxiv_category = None
    primary_report_num = info.find(MARC+'datafield[@tag="037"]')
    if primary_report_num != None and \
       primary_report_num.findtext(MARC+'subfield[@code="9"]') == 'arXiv':
        arxiv_id = primary_report_num.findtext(MARC+'subfield[@code="a"]')
        arxiv_category = \
            primary_report_num.findtext(MARC+'subfield[@code="c"]')

    abstract = info.findtext(
        MARC+'datafield[@tag="520"]/'+
        MARC+'subfield[@code="a"]')

    # Authors
    first_author = info.find(MARC+'datafield[@tag="100"]')

    if first_author == None:
        print "%s : skipping (missing author)" % inspire_id
        return
        
    if dry_run:
        print "%s : added : %s : '%s'" % (inspire_id, created, title)
        return
    
    add_author(cursor, inspire_id, first_author)

    for author_elem in info.iterfind(MARC+'datafield[@tag="700"]'):
        add_author(cursor, inspire_id, author_elem)

    # References
    references = info.findall(
        MARC+'datafield[@tag="999"]/'+
        MARC+'subfield[@code="0"]')
    references = [elem.text for elem in references]

    for ref_id in references:
        cursor.execute('''INSERT INTO inspire_references VALUES (?,?)''',
                  (inspire_id, ref_id))

    cursor.execute('''INSERT INTO inspire_papers VALUES (?,?,?,?,?,?,?,?)''',
              (inspire_id, arxiv_id, arxiv_category,
               title, datestamp, abstract, created, doi))

    print "%s : added : %s : '%s'" % (inspire_id, created, title)

def harvest_xml(conn, cursor, root, dry_run=False):
    """
    Harvest MARC records out of the given XML root.
    Return True if any records were found
    """
    all_records = root.find(OAI+'ListRecords')

    if all_records is None:
        return False

    for record in all_records.findall(OAI+"record"):
        add_record(cursor, record, dry_run)

    if not dry_run:
        conn.commit()
    return True

def harvest_from_file(conn, cursor, filename, dry_run):
    f = open(filename, 'r')
    xml = f.read()
    f.close()
    root = ET.fromstring(xml)
    harvest_xml(conn, cursor, root, dry_run)

def save_resumption_token(token):
    with open(resumption_token_file, 'w') as f:
        f.write(token)

def clear_resumption_token(token):
    if os.path.isfile(resumption_token_file):
        os.remove(resumption_token_file)

def load_resumption_token():
    if os.path.isfile(resumption_token_file):
        with open(resumption_token_file, 'r') as f:
            return f.read().strip()
    else:
        return None

def harvest(conn, cursor):
    resumption_token = load_resumption_token()

    if resumption_token == None:
        url = (base_harvesting_url +
            "from=%s&metadataPrefix=%s&set=%s"
            % (start_date, metadata_prefix, data_set))
    else:
        url = base_harvesting_url + "resumptionToken=%s" % resumption_token
    
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

        # Save XML for debugging purposes
        f = open('latest-marc.xml', 'w')
        f.write(xml)
        f.close()

        root = ET.fromstring(xml)

        records_found = harvest_xml(conn, cursor, root)

        if not records_found:
            break

        # The list of articles returned by the API comes in chunks of
        # 1000 articles. The presence of a resumptionToken tells us that
        # there is more to be fetched.
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            clear_resumption_token()
            break
        else:
            save_resumption_token(token.text)
            url = base_harvesting_url + "resumptionToken=%s" % (token.text)
            
def find_latest_start_date(cursor):
    cursor.execute("SELECT MAX(datestamp) FROM inspire_papers")
    start_date = cursor.fetchone()

    if start_date == None:
        print "Not found, starting at earliest datestamp."
        return earliest_datestamp
    else:
        # Increase by one day to avoid re-downloading the papers from
        # the last day
        start_date = start_date[0]
        start_date = re.sub(r'T.*', r'', start_date)
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        start_date = start_date + datetime.timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")
        return start_date

def main():
    conn = sql.connect(db_filename)
    cursor = conn.cursor()

    if not os.path.isfile(db_filename):
        print "Database file %s does not exit" % db_filename
        exit(1)

    parser = argparse.ArgumentParser(description="Harvest INSPIRE metadata")
    parser.add_argument('-n', '--dry-run',
                        help='only print, don\'t store anything',
                        action='store_true')
    parser.add_argument('-f', '--file',
                        help='read xml from given file instead of downloading'
                        )
    args = parser.parse_args()

    if args.dry_run and args.file == None:
        print "Dry run only possible with --file"
        exit(1)

    start_date = find_latest_start_date(cursor)
    print "Start date: %s" % start_date

    if args.file == None:
        harvest(conn, cursor)
    else:
        harvest_from_file(conn, cursor, args.file, args.dry_run)
    conn.close()
    
if __name__ == '__main__':
    main()

