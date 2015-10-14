/*
	What are commented are either PK or UNIQE thus index automatically
*/

CREATE INDEX pub_author_aid_index ON pub_author (aid);
CREATE INDEX pub_author_pubid_index ON pub_author (pubid);

-- CREATE INDEX publication_pubid_index ON publication (pubid);
CREATE INDEX publication_year_index ON publication (year);
CREATE INDEX publication_type_index ON publication (type);

-- CREATE INDEX article_pubid_index ON article (pubid);
-- CREATE INDEX book_pubid_index ON book (pubid);
-- CREATE INDEX inproceedings_pubid_index ON inproceedings (pubid);
-- CREATE INDEX incollection_pubid_index ON incollection (pubid);

CREATE INDEX article_journal_index ON article (journal);
CREATE INDEX inproceedings_booktitle_index ON inproceedings (booktitle);