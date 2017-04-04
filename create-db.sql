--
-- Creates a database for holding hep-th papers, including data from
-- arXiv and INSPIRE.
--

-- arXiv tables
CREATE TABLE arxiv_papers
	(id text, oai_id text, title text, datestamp text, abstract text,
	categories text, created text, doi text, comments text);

CREATE TABLE arxiv_authors
	(first_name text, last_name text, affiliation text, paper_id text);

CREATE UNIQUE INDEX arxiv_id on arxiv_papers (id);
CREATE INDEX arxiv_datestamp on arxiv_papers (datestamp);
CREATE INDEX arxiv_paper_id on arxiv_authors (paper_id);
CREATE INDEX arxiv_author_name on arxiv_authors (last_name, first_name);

-- INSPIRE tables
CREATE TABLE inspire_papers
	(id text, arxiv_id text, arxiv_category text, title text,
	datestamp text, abstract text, publication_date text, doi text);

CREATE TABLE inspire_references
	(id text, ref_id text);

CREATE TABLE inspire_authors
    (first_name text, last_name text, paper_id text);

CREATE UNIQUE INDEX inspire_paper_id on inspire_papers (id);
CREATE INDEX inspire_arxiv_id on inspire_papers (arxiv_id);
CREATE INDEX inspire_datestamp on inspire_papers (datestamp);
CREATE INDEX inspire_authors_paper_id on inspire_authors (paper_id);
CREATE INDEX inspire_author_name on inspire_authors (last_name, first_name);
CREATE INDEX inspire_ref_origin_id on inspire_references (id);
CREATE INDEX inspire_ref_target_id on inspire_references (ref_id);
