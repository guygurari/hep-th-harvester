CREATE TABLE arxiv_papers
	(id text, oai_id text, title text, datestamp text, abstract text,
	categories text, created text, doi text, comments text);

CREATE TABLE arxiv_authors
	(first_name text, last_name text, affiliation text, paper_id text);

CREATE UNIQUE INDEX arxiv_id on arxiv_papers (id);
CREATE INDEX arxiv_datestamp on arxiv_papers (datestamp);
CREATE INDEX arxiv_paper_id on arxiv_authors (paper_id);
CREATE INDEX arxiv_author_name on arxiv_authors (last_name, first_name);
