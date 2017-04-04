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
db_filename = "../hep-th.sqlite"

working_state_dir = 'working-state'
resumption_token_file = '%s/inspire-resumption-token.txt' % working_state_dir
latest_marc_file = '%s/latest-marc.xml' % working_state_dir

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
               
def find_publication_date(info):
    """Find the publication date of the given record."""
    # Possible xpaths for relevant dates
    # (publication date, thesis defense date, preprint date, ...)
    date_xpaths = [
        MARC+'datafield[@tag="961"]/'+MARC+'subfield[@code="x"]',
        MARC+'datafield[@tag="502"]/'+MARC+'subfield[@code="d"]',
        MARC+'datafield[@tag="269"]/'+MARC+'subfield[@code="c"]',
        MARC+'datafield[@tag="260"]/'+MARC+'subfield[@code="c"]',
        MARC+'datafield[@tag="773"]/'+MARC+'subfield[@code="d"]',
        MARC+'datafield[@tag="773"]/'+MARC+'subfield[@code="y"]',
        ]

    for path in date_xpaths:
        pub_date = info.findtext(path)
        if pub_date != None:
            return pub_date

    return None
    
def add_record(cursor, record, dry_run=False):
    header = record.find(OAI+'header')
    datestamp = None

    if header != None:
        status = header.get('status')

        if status != None and status == 'deleted':
            print "skipping (deleted record)"
            return

        #oai_id = header.find(OAI+'identifier').text
        datestamp = header.findtext(OAI+'datestamp')
    
    # This shows up in OAI dumps of MARC XML
    info = record.find(OAI+'metadata/'+MARC+'record')

    if info == None:
        # This shows up in single record MARC XML
        info = record.find(MARC+"record")

    if info == None:
        raise ValueError("Cannot find <record> element")

    inspire_id = info.findtext(MARC+'controlfield[@tag="001"]')

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

    doi = info.findtext(
        MARC+'datafield[@tag="024"]/'+
        MARC+'subfield[@code="a"]')

    pub_date = find_publication_date(info)

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
        print "%s : added : %s : '%s'" % (inspire_id, pub_date, title)
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
               title, datestamp, abstract, pub_date, doi))

    print "%s : added : %s : '%s'" % (inspire_id, pub_date, title)

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

def clear_resumption_token():
    if os.path.isfile(resumption_token_file):
        os.remove(resumption_token_file)

def load_resumption_token():
    if os.path.isfile(resumption_token_file):
        with open(resumption_token_file, 'r') as f:
            return f.read().strip()
    else:
        return None

def read_url(url):
    """Read the text response from the given URL. Retry as needed."""
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

        return response.read()

def harvest(conn, cursor):
    start_date = find_latest_start_date(cursor)
    print "Harvesting (start date: %s)" % start_date

    resumption_token = load_resumption_token()

    if resumption_token == None:
        url = (base_harvesting_url +
            "from=%s&metadataPrefix=%s&set=%s"
            % (start_date, metadata_prefix, data_set))
    else:
        url = base_harvesting_url + "resumptionToken=%s" % resumption_token
    
    while True:
        xml = read_url(url)

        # Save XML for debugging purposes
        f = open(latest_marc_file, 'w')
        f.write(xml)
        f.close()

        print ">>> parsing XML\n"
        root = ET.fromstring(xml)

        print ">>> adding records\n"
        records_found = harvest_xml(conn, cursor, root)

        if not records_found:
            break

        # The list of articles returned by the API comes in chunks of
        # 1000 articles. The presence of a resumptionToken tells us that
        # there is more to be fetched.
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            print "\nNo resumption token given, we are done."
            clear_resumption_token()
            break
        else:
            save_resumption_token(token.text)
            url = base_harvesting_url + "resumptionToken=%s" % (token.text)

def harvest_single_record(conn, cursor, record_id):
    url = "https://inspirehep.net/record/%s?of=xm" % record_id
    xml = read_url(url)
    root = ET.fromstring(xml)
    add_record(cursor, root)
    conn.commit()

def delete_record(conn, cursor, record_id):
    if not is_in_db(cursor, record_id):
        print "Record %s not found" % record_id
        return
        
    print "Deleting record %s" % record_id
    cursor.execute('''DELETE FROM inspire_papers WHERE id=?''',
                   (record_id,))
    cursor.execute('''DELETE FROM inspire_authors WHERE paper_id=?''',
                   (record_id,))
    cursor.execute('''DELETE FROM inspire_references WHERE id=?''',
                   (record_id,))
    conn.commit()
    
def find_latest_start_date(cursor):
    cursor.execute("SELECT MAX(datestamp) FROM inspire_papers")
    start_date = cursor.fetchone()

    if start_date == None:
        print "Not found, starting at earliest datestamp."
        return earliest_datestamp
    else:
        start_date = start_date[0]
        start_date = re.sub(r'T.*', r'', start_date)
        #start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        #start_date = start_date + datetime.timedelta(days=1)
        #start_date = start_date.strftime("%Y-%m-%d")
        return start_date

def main():
    conn = sql.connect(db_filename)
    cursor = conn.cursor()

    if not os.path.isfile(db_filename):
        print "Database file %s not found" % db_filename
        exit(1)

    if not os.path.isdir(working_state_dir):
        os.mkdir(working_state_dir)

    parser = argparse.ArgumentParser(description="Harvest INSPIRE metadata")
    parser.add_argument('-n', '--dry-run',
                        help='only print, don\'t store anything',
                        action='store_true')
    parser.add_argument('--file',
                        help='read xml from given file instead of downloading'
                        )
    parser.add_argument('-r', '--record',
                        help='download a single record'
                        )
    parser.add_argument('-d', '--delete',
                        help='delete a single record'
                        )
    parser.add_argument('--force',
                        help='force re-download even if record exists',
                        action='store_true')
    args = parser.parse_args()

    if args.dry_run and args.file == None:
        print "Dry run only possible with --file"
        exit(1)

    if args.file != None:
        harvest_from_file(conn, cursor, args.file, args.dry_run)
    elif args.record != None:
        if args.force and is_in_db(cursor, args.record):
            delete_record(conn, cursor, args.record)
        harvest_single_record(conn, cursor, args.record)
    elif args.delete != None:
        delete_record(conn, cursor, args.delete)
    else:
        harvest(conn, cursor)

    # Commit before closing, just in case we forgot
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    main()

