-- 1

( SELECT 'article' AS type, count(*) AS total_num
  FROM article
) 
UNION ALL
( SELECT 'book' AS type, count(*) AS total_num
  FROM book
)
UNION ALL
( SELECT 'incollection' AS type, count(*) AS total_num
  FROM incollection
)
UNION ALL
( SELECT 'inproceedings' AS type, count(*) AS total_num
  FROM inproceedings
);

-- 2A)
-- index on pub_author.aid (group by)??
-- index on author.aid (join)
-- use view instead of with and build index?

WITH publication_count AS (
	SELECT aid, count(pid) AS total 
	FROM pub_author
	GROUP BY aid
	ORDER BY total DESC
	LIMIT 10
	)

SELECT name
FROM publication_count JOIN author USING aid;

-- 2B)
-- index on pub_author.pid
-- index on pub_author.aid (group by)??
-- index on author.aid (join)
-- use view instead of with and build index?

WITH page_count AS (
	SELECT aid, SUM(total_page) AS total 
	FROM pub_author JOIN publication USING pubid
	GROUP BY aid
	ORDER BY total DESC
	LIMIT 10
	)

SELECT name
FROM page_count JOIN author USING aid;

-- 3A)
-- create index for view??
-- index on author.aid, author.name
-- index on pub_author.aid, pub_author.pid
-- index on publication.pid, publication.year
-- index on article.pid, book.pid, incollection.pid, inproceedings.pid

DROP VIEW special_publication IF EXIST
CREATE VIEW special_publication AS(
	SELECT publication.pubid as pubid
	FROM author JOIN pub_author ON (author.aid = pub_author.aid AND author.name = X)
				JOIN publication ON (pub_author.pid = publication.pid AND year = 2012)
)

SELECT pubid, pubkey, title, year, journal, month, volumn, number 
FROM article JOIN special_publication USING pubid;

SELECT pubid, pubkey, title, year, publisher, isbn
FROM book JOIN special_publication USING pubid;

SELECT pubid, pubkey, title, year, booktitle, publisher, isbn
FROM incollection JOIN special_publication USING pubid;

SELECT pubid, pubkey, title, year, booktitle, editor
FROM inproceedings JOIN special_publication USING pubid;

-- 3B)
-- same as 3A
-- index on publication.type
-- index on article.journal
-- index on inproceedings.booktitle

DROP VIEW special_publication2 IF EXIST
CREATE VIEW special_publication2 AS(
	SELECT publication.pubid as pubid
	FROM author JOIN pub_author ON (author.aid = pub_author.aid AND author.name = X)
				JOIN publication ON (pub_author.pid = publication.pid AND year = Y AND type = 'conf')
)

SELECT article.pubid, pubkey, title, year, journal, month, volumn, number 
FROM article JOIN special_publication2 ON (article.journal = Z AND article.pubid = special_publication2.pubid);

SELECT publication.pubid, pubkey, title, year, booktitle, editor
FROM inproceedings JOIN special_publication2 ON (inproceedings.booktitle = Z AND inproceedings.pubid = special_publication2.pubid);

-- 4A)
-- index on article.journal, article.pid
-- index on pub_author.pid
-- index on inproceedings.booktitle, inproceedings.pid
-- index on author.aid

WITH PVLDB_author AS(
	SELECT aid
	FROM pub_author JOIN article ON (journal = 'PVLDB' AND pub_author.pid = article.pid)
	UNION ALL
	SELECT aid
	FROM pub_author JOIN inproceedings ON (booktitle = 'PVLDB' AND pub_author.pid = inproceedings.pid)
)
CREATE VIEW PVLDB_count AS(
	SELECT aid
	FROM PVLDB_author
	GROUP BY aid
	HAVING count(aid) >= 10
);

WITH KDD_author AS(
	SELECT aid
	FROM pub_author JOIN article ON (journal = 'KDD' AND pub_author.pid = article.pid)
	UNION ALL
	SELECT aid
	FROM pub_author JOIN inproceedings ON (booktitle = 'KDD' AND pub_author.pid = inproceedings.pid)
)
CREATE VIEW KDD_count AS(
	SELECT aid, count(*) AS total
	FROM KDD_author
	GROUP BY aid
);

WITH P10K5 AS(
	SELECT aid FROM PVLDB_count
	INTERSECT
	SELECT aid FROM KDD_count WHERE total >= 5
)
SELECT name
FROM author JOIN P10K5 ON (author.aid = P10K5.aid);

-- 4B)
-- same as 4A

WITH P10K0 AS(
	SELECT aid FROM PVLDB_count
	EXCEPT
	SELECT aid FROM KDD_count
)
SELECT name
FROM author JOIN P10K0 ON (author.aid = P10K0.aid);


-- 5
-- index on publication.year

CREATE VIEW decade70 AS ( SELECT pid FROM publication WHERE year >= 1970 AND year <=1979 );
CREATE VIEW decade80 AS ( SELECT pid FROM publication WHERE year >= 1980 AND year <=1989 );
CREATE VIEW decade90 AS ( SELECT pid FROM publication WHERE year >= 1990 AND year <=1999 );
CREATE VIEW decade00 AS ( SELECT pid FROM publication WHERE year >= 2000 AND year <=2009 );
CREATE VIEW decade10 AS ( SELECT pid FROM publication WHERE year >= 2010 AND year <=2019 );

SELECT '1970 - 1979', SUM(pid) FROM decade70;
SELECT '1980 - 1989', SUM(pid) FROM decade80;
SELECT '1990 - 1999', SUM(pid) FROM decade90;
SELECT '2000 - 2009', SUM(pid) FROM decade00;
SELECT '2010 - 2019', SUM(pid) FROM decade10;

-- 6
-- index on pub_author.aid, pub_author.pubid

WITH decade70_top_author AS(
	SELECT aid, SUM(pid) AS total
	FROM decade70 JOIN pub_author USING pubid
	GROUP BY aid
)
SELECT '1970 - 1979', name 
FROM decade70_top_author JOIN author ON (total = MAX(total) AND decade70_top_author.aid = author.aid);

WITH decade80_top_author AS(
	SELECT aid, SUM(pid) AS total
	FROM decade80 JOIN pub_author USING pubid
	GROUP BY aid
)
SELECT '1980 - 1989', name 
FROM decade80_top_author JOIN author ON (total = MAX(total) AND decade80_top_author.aid = author.aid);

WITH decade90_top_author AS(
	SELECT aid, SUM(pid) AS total
	FROM decade90 JOIN pub_author USING pubid
	GROUP BY aid
)
SELECT '1990 - 1999', name 
FROM decade90_top_author JOIN author ON (total = MAX(total) AND decade90_top_author.aid = author.aid);

WITH decade00_top_author AS(
	SELECT aid, SUM(pid) AS total
	FROM decade00 JOIN pub_author USING pubid
	GROUP BY aid
)
SELECT '2000 - 2009', name 
FROM decade00_top_author JOIN author ON (total = MAX(total) AND decade00_top_author.aid = author.aid);

WITH decade10_top_author AS(
	SELECT aid, SUM(pid) AS total
	FROM decade10 JOIN pub_author USING pubid
	GROUP BY aid
)
SELECT '2010 - 2019', name 
FROM decade10_top_author JOIN author ON (total = MAX(total) AND decade10_top_author.aid = author.aid);

-- 7
-- index on pub_author.aid, pub_author.pid

WITH collaborator AS(
	SELECT p1.aid AS aid, p2.aid AS col_aid
	FROM pub_author p1 JOIN pub_author p2 ON (p1.pubid = p2.pubid AND NOT p1.aid = p2.aid)
), col_count AS(
	SELECT aid, SUM(col_aid) AS total
	FROM collaborator
	GROUP BY aid
)
SELECT name
FROM col_count JOIN author ON (total = MAX(total) AND author.aid = col_count.aid);


