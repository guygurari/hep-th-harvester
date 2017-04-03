CREATE TABLE inspire_papers
	(id text, arxiv_id text, arxiv_category text, title text,
	datestamp text, abstract text, publication_date text, doi text);

CREATE TABLE inspire_references
	(id text, ref_id text);

CREATE TABLE inspire_authors
    (first_name text, last_name text, paper_id text);

CREATE UNIQUE INDEX inspire_paper_id on inspire_papers (id);
CREATE INDEX inspire_datestamp on inspire_papers (datestamp);
CREATE INDEX inspire_authors_paper_id on inspire_authors (paper_id);
CREATE INDEX inspire_author_name on inspire_authors (last_name, first_name);
CREATE INDEX inspire_ref_origin_id on inspire_references (id);
CREATE INDEX inspire_ref_target_id on inspire_references (ref_id);
