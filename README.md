# hep-th harvester

Harvests theoretical high-energy physics papers from arXiv and INSPIRE.
Uses the official OAI interface for bulk harvesting.
Paper metadata (title, author, references, etc.) is stored in an sqlite
database. PDF files are stored in S3 (see below for details).

These are the main scripts:

* `db/create-db` : Create the database `hep-th.sqlite`

* `arxiv/harvest-arxiv-metadata.py` : Download the full hep-th arXiv into 
  the database. The first run takes a long time (possibly hours).
  Subsequent runs will only download new papers.

* `inspire/harvest-inspire-metadata.py` : Download the full INSPIRE
  hep-th records into the database. This includes references for each paper.
  First run can take a day or more. If stopped and re-started during this
  first (long) run, download resumes from the most recent paper.
  Subsequent runs only download new papers.

* `pdfs/get-and-filter-arxiv-pdfs.pl` : Download PDFs from an arXiv S3
  bucket, and keep only the `hep-th` ones.  Before running this
  script, the list of PDFs should be downloaded by running
  `download-arxiv-pdf-list`. *Note:* The arXiv bucket is 'requester
  pays'. To avoid bandwidth costs, it is best to run this script on an
  EC2 instance in the bucket's region, US-East (Virginia).

* `pdfs/upload-to-s3.pl` : Continuously upload the downloaded PDFs to
  S3. The upload bucket can be configured in `upload-to-s3.pl`.
  Can be run concurrently with `pdfs/get-and-filter-arxiv-pdfs.pl` to
  keep local disk usage low. This is useful on micro EC2 instances which 
  don't have a lot of local storage.
