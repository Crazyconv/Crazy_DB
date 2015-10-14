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
	SELECT aid, count(pubid) AS total 
	FROM pub_author
	GROUP BY aid
	ORDER BY total DESC
	LIMIT 10
	)

SELECT name
FROM publication_count JOIN author USING (aid);

-- 2B)
-- index on pub_author.pubid
-- index on publication.pubid (join)
-- index on pub_author.aid (group by)??
-- index on author.aid (join)
-- use view instead of with and build index?

WITH page_count AS (
	SELECT aid, SUM(total_page) AS total 
	FROM pub_author JOIN publication USING (pubid)
	GROUP BY aid
	ORDER BY total DESC
	LIMIT 10
	)

SELECT name
FROM page_count JOIN author USING (aid);

-- 3A)
-- create index for view??
-- index on author.aid, author.name
-- index on pub_author.aid, pub_author.pubid
-- index on publication.pubid, publication.year
-- index on article.pubid, book.pubid, incollection.pubid, inproceedings.pubid

DROP VIEW IF EXISTS special_publication;
CREATE VIEW special_publication AS(
	SELECT publication.*
	FROM author JOIN pub_author ON (author.aid = pub_author.aid AND author.name = 'Yang Yang')
				JOIN publication ON (pub_author.pubid = publication.pubid AND year = 2012)
);

SELECT pubid, pubkey, title, year, journal, month, volume, number 
FROM article JOIN special_publication USING (pubid);

SELECT pubid, pubkey, title, year, publisher, isbn
FROM book JOIN special_publication USING (pubid);

SELECT pubid, pubkey, title, year, booktitle, publisher, isbn
FROM incollection JOIN special_publication USING (pubid);

SELECT pubid, pubkey, title, year, booktitle, editor
FROM inproceedings JOIN special_publication USING (pubid);

-- 3B)
-- same as 3A
-- index on publication.type
-- index on article.journal
-- index on inproceedings.booktitle

DROP VIEW IF EXISTS special_publication2;
CREATE VIEW special_publication2 AS(
	SELECT publication.*
	FROM author JOIN pub_author ON (author.aid = pub_author.aid AND author.name = 'Wei Wang')
				JOIN publication ON (pub_author.pubid = publication.pubid AND year = 2009 AND type = 'conf')
);

SELECT article.pubid, pubkey, title, year, journal, month, volume, number 
FROM article JOIN special_publication2 ON (article.journal = 'CSCWD' AND article.pubid = special_publication2.pubid);

SELECT inproceedings.pubid, pubkey, title, year, booktitle, editor
FROM inproceedings JOIN special_publication2 ON (inproceedings.booktitle = 'CSCWD' AND inproceedings.pubid = special_publication2.pubid);

-- 4A)
-- index on article.journal, article.pubid
-- index on pub_author.pubid
-- index on inproceedings.booktitle, inproceedings.pubid
-- index on author.aid
DROP VIEW IF EXISTS PVLDB_count;
CREATE VIEW PVLDB_count AS(
	SELECT aid
	FROM (
		SELECT aid
		FROM pub_author JOIN article ON (journal = 'PVLDB' AND pub_author.pubid = article.pubid)
		UNION ALL
		SELECT aid
		FROM pub_author JOIN inproceedings ON (booktitle = 'PVLDB' AND pub_author.pubid = inproceedings.pubid)
	) AS PVLDB_author
	GROUP BY aid
	HAVING count(aid) >= 10
);

DROP VIEW IF EXISTS KDD_count;
CREATE VIEW KDD_count AS(
	SELECT aid, count(*) AS total
	FROM (
		SELECT aid
		FROM pub_author JOIN article ON (journal = 'KDD' AND pub_author.pubid = article.pubid)
		UNION ALL
		SELECT aid
		FROM pub_author JOIN inproceedings ON (booktitle = 'KDD' AND pub_author.pubid = inproceedings.pubid)
	) AS KDD_author
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
DROP VIEW IF EXISTS decade70;
DROP VIEW IF EXISTS decade80;
DROP VIEW IF EXISTS decade90;
DROP VIEW IF EXISTS decade00;
DROP VIEW IF EXISTS decade10;

CREATE VIEW decade70 AS ( SELECT pubid FROM publication WHERE year >= 1970 AND year <=1979 );
CREATE VIEW decade80 AS ( SELECT pubid FROM publication WHERE year >= 1980 AND year <=1989 );
CREATE VIEW decade90 AS ( SELECT pubid FROM publication WHERE year >= 1990 AND year <=1999 );
CREATE VIEW decade00 AS ( SELECT pubid FROM publication WHERE year >= 2000 AND year <=2009 );
CREATE VIEW decade10 AS ( SELECT pubid FROM publication WHERE year >= 2010 AND year <=2019 );

( SELECT '1970 - 1979' AS decade, count(pubid) FROM decade70 )
UNION ALL
( SELECT '1980 - 1989' AS decade, count(pubid) FROM decade80 )
UNION ALL
( SELECT '1990 - 1999' AS decade, count(pubid) FROM decade90 )
UNION ALL
( SELECT '2000 - 2009' AS decade, count(pubid) FROM decade00 )
UNION ALL
( SELECT '2010 - 2019' AS decade, count(pubid) FROM decade10 );

-- 6
-- index on pub_author.aid, pub_author.pubid
(
	WITH decade70_top_author AS(
		SELECT aid, count(pubid) AS total
		FROM decade70 JOIN pub_author USING (pubid)
		GROUP BY aid
	)
	SELECT '1970 - 1979' AS decade, name 
	FROM decade70_top_author JOIN author ON 
		(total = (SELECT MAX(total) FROM decade70_top_author) AND decade70_top_author.aid = author.aid)
) UNION ALL (
	WITH decade80_top_author AS(
		SELECT aid, count(pubid) AS total
		FROM decade80 JOIN pub_author USING (pubid)
		GROUP BY aid
	)
	SELECT '1980 - 1989' AS decade, name 
	FROM decade80_top_author JOIN author ON 
		(total = (SELECT MAX(total) FROM decade80_top_author) AND decade80_top_author.aid = author.aid)
) UNION ALL (
	WITH decade90_top_author AS(
		SELECT aid, count(pubid) AS total
		FROM decade90 JOIN pub_author USING (pubid)
		GROUP BY aid
	)
	SELECT '1990 - 1999' AS decade, name 
	FROM decade90_top_author JOIN author ON 
		(total = (SELECT MAX(total) FROM decade90_top_author) AND decade90_top_author.aid = author.aid)
) UNION ALL (
	WITH decade00_top_author AS(
		SELECT aid, count(pubid) AS total
		FROM decade00 JOIN pub_author USING (pubid)
		GROUP BY aid
	)
	SELECT '2000 - 2009' AS decade, name 
	FROM decade00_top_author JOIN author ON 
		(total = (SELECT MAX(total) FROM decade00_top_author) AND decade00_top_author.aid = author.aid)
) UNION ALL (
	WITH decade10_top_author AS(
		SELECT aid, count(pubid) AS total
		FROM decade10 JOIN pub_author USING (pubid)
		GROUP BY aid
	)
	SELECT '2010 - 2019', name 
	FROM decade10_top_author JOIN author ON 
		(total = (SELECT MAX(total) FROM decade10_top_author) AND decade10_top_author.aid = author.aid)
)
-- 7
-- index on pub_author.aid, pub_author.pubid

WITH collaborator AS(
	SELECT p1.aid AS aid, p2.aid AS col_aid
	FROM pub_author p1 JOIN pub_author p2 ON (p1.pubid = p2.pubid AND NOT p1.aid = p2.aid)
), col_count AS(
	SELECT aid, count(DISTINCT col_aid) AS total
	FROM collaborator
	GROUP BY aid
)
SELECT name
FROM col_count JOIN author ON 
	(total = (SELECT MAX(total) FROM col_count) AND author.aid = col_count.aid);


