-- Query 1

(SELECT 'Article' AS type, count(*) AS num FROM article)
UNION
(SELECT 'Book' AS type, count(*) AS num FROM book)
UNION
(SELECT 'Incollection' AS type, count(*) AS num FROM incollection)
UNION
(SELECT 'Inproceeding' AS type, count(*) AS num FROM inproceedings);

----------
-- Query 2A

DROP VIEW IF EXISTS pub_count_2A CASCADE;
DROP VIEW IF EXISTS pub_rank_2A;

CREATE VIEW pub_count_2A AS(
  SELECT aid, count(*) AS num_pub
  FROM pub_author
  GROUP BY aid
);

CREATE VIEW pub_rank_2A AS(
  SELECT aid, rank() OVER (ORDER BY num_pub DESC)
	FROM pub_count_2A
);

SELECT pub_rank_2A.rank, author.name
FROM pub_rank_2A
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
-- Query 2B

DROP VIEW IF EXISTS pub_count_2B CASCADE;
DROP VIEW IF EXISTS pub_rank_2B;
CREATE VIEW pub_count_2B AS(
  SELECT pub_author.aid, SUM(total_page) AS total_page
  FROM pub_author
  JOIN publication USING (pubid)
  GROUP BY aid
);

CREATE VIEW pub_rank_2B AS(
  SELECT aid, rank() OVER (ORDER BY total_page DESC)
  FROM pub_count_2B
);

SELECT pub_rank_2B.rank, author.name
FROM pub_rank_2B
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
-- Query 3 : Author: Yan Zhang
-- Query 3A

DROP VIEW IF EXISTS pub_info_3A;
CREATE VIEW pub_info_3A AS(
  SELECT author.name, publication.*
  FROM pub_author
  JOIN author ON pub_author.aid = author.aid
  JOIN publication ON pub_author.pubid = publication.pubid
  WHERE author.name = 'Yan Zhang' and publication.year = 2012
);

SELECT * FROM pub_info_3A
LEFT JOIN article ON pub_info_3A.pubid = article.pubid
LEFT JOIN book ON pub_info_3A.pubid = book.pubid
LEFT JOIN incollection ON pub_info_3A.pubid = incollection.pubid
LEFT JOIN inproceedings ON pub_info_3A.pubid = inproceedings.pubid;

----------
-- Query 3B

DROP VIEW IF EXISTS pub_info_3B;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';

----------
--Query 4A:

DROP VIEW IF EXISTS PVLDB_4;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
);

DROP VIEW IF EXISTS KDD_4A;
CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
SELECT author.name
FROM PVLDB_4
JOIN KDD_4A ON PVLDB_4.aid = KDD_4A.aid
JOIN author ON PVLDB_4.aid = author.aid
Where PVLDB_4.PVLDB_num >= 10 and KDD_4A.KDD_num >= 5;

----------
--Query 4B:
SELECT author.name
FROM PVLDB_4
JOIN KDD_4A ON PVLDB_4.aid = KDD_4A.aid
JOIN author ON PVLDB_4.aid = author.aid
Where PVLDB_4.PVLDB_num >= 10 and KDD_4A.KDD_num is NULL;

----------
--Query 5:
DROP VIEW IF EXISTS decade_1970;
DROP VIEW IF EXISTS decade_1980;
DROP VIEW IF EXISTS decade_1990;
DROP VIEW IF EXISTS decade_2000;
DROP VIEW IF EXISTS decade_2010;

CREATE VIEW decade_1970 AS(
  SELECT pubid FROM publication
  WHERE year >= 1970 and year <= 1979
);

CREATE VIEW decade_1980 AS(
  SELECT pubid FROM publication
  WHERE year >= 1980 and year <= 1989
);

CREATE VIEW decade_1990 AS(
  SELECT pubid FROM publication
  WHERE year >= 1990 and year <= 1999
);

CREATE VIEW decade_2000 AS(
  SELECT pubid FROM publication
  WHERE year >= 2000 and year <= 2009
);

CREATE VIEW decade_2010 AS(
  SELECT pubid FROM publication
  WHERE year >= 2010 and year <= 2019
);

(SELECT '1970-1979' AS decade, count(*) AS num FROM decade_1970)
UNION
(SELECT '1980-1989' AS decade, count(*) AS num FROM decade_1980)
UNION
(SELECT '1990-1999' AS decade, count(*) AS num FROM decade_1990)
UNION
(SELECT '2000-2009' AS decade, count(*) AS num FROM decade_2000)
UNION
(SELECT '2010-2019' AS decade, count(*) AS num FROM decade_2010);

----------
-- Query 6:
DROP VIEW IF EXISTS decade_1970_top_author;
DROP VIEW IF EXISTS decade_1980_top_author;
DROP VIEW IF EXISTS decade_1990_top_author;
DROP VIEW IF EXISTS decade_2000_top_author;
DROP VIEW IF EXISTS decade_2010_top_author;


CREATE VIEW decade_1970_top_author AS(
  SELECT aid, count(pubid) AS pub_num
  FROM decade_1970 JOIN pub_author USING (pubid)
  GROUP BY aid
);

CREATE VIEW decade_1980_top_author AS(
  SELECT aid, count(pubid) AS pub_num
  FROM decade_1980 JOIN pub_author USING (pubid)
  GROUP BY aid
);

CREATE VIEW decade_1990_top_author AS(
  SELECT aid, count(pubid) AS pub_num
  FROM decade_1990 JOIN pub_author USING (pubid)
  GROUP BY aid
);

CREATE VIEW decade_2000_top_author AS(
  SELECT aid, count(pubid) AS pub_num
  FROM decade_2000 JOIN pub_author USING (pubid)
  GROUP BY aid
);

CREATE VIEW decade_2010_top_author AS(
  SELECT aid, count(pubid) AS pub_num
  FROM decade_2010 JOIN pub_author USING (pubid)
  GROUP BY aid
);

(
  SELECT '1970 - 1979' AS decade, name 
  FROM decade_1970_top_author JOIN author ON 
    (pub_num = (SELECT MAX(pub_num) FROM decade_1970_top_author) AND decade_1970_top_author.aid = author.aid)
) UNION ALL (
  SELECT '1980 - 1989' AS decade, name 
  FROM decade_1980_top_author JOIN author ON 
    (pub_num = (SELECT MAX(pub_num) FROM decade_1980_top_author) AND decade_1980_top_author.aid = author.aid)
) UNION ALL (
  SELECT '1990 - 1999' AS decade, name 
  FROM decade_1990_top_author JOIN author ON 
    (pub_num = (SELECT MAX(pub_num) FROM decade_1990_top_author) AND decade_1990_top_author.aid = author.aid)
) UNION ALL (
  SELECT '2000 - 2009' AS decade, name 
  FROM decade_2000_top_author JOIN author ON 
    (pub_num = (SELECT MAX(pub_num) FROM decade_2000_top_author) AND decade_2000_top_author.aid = author.aid)
) UNION ALL (
  SELECT '2010 - 2019', name 
  FROM decade_2010_top_author JOIN author ON 
    (pub_num = (SELECT MAX(pub_num) FROM decade_2010_top_author) AND decade_2010_top_author.aid = author.aid)
);

----------
--Query 7
DROP VIEW IF EXISTS collaborator;
DROP VIEW IF EXISTS collaborator_counts;


CREATE VIEW collaborator AS(
  SELECT a.aid, b.aid as colla_id
  FROM pub_author a
  JOIN pub_author b ON a.pubid = b.pubid and NOT a.aid = b.aid
);

CREATE VIEW collaborator_count AS(
  SELECT aid, count(DISTINCT colla_id) AS colla_num
  FROM collaborator
  GROUP BY aid
  ORDER BY colla_num DESC
);

SELECT author.name
FROM collaborator_count
JOIN author
ON collaborator_count.aid = author.aid AND colla_num = (SELECT MAX(colla_num) FROM collaborator_count);

-- Query 8
-- select the authors who have writen more than 500 pages of publication
DROP VIEW IF EXISTS page_count_8;

CREATE VIEW page_count_8 AS(
  SELECT pub_author.aid, SUM(total_page) AS total_page
  FROM pub_author
  JOIN publication USING (pubid)
  GROUP BY aid
);

SELECT author.name, total_page
FROM page_count_8
JOIN author USING (aid)
WHERE total_page >= 4000
ORDER by total_page DESC;

-- Query 9
-- select the top ten prolistic authors in all the conferences
DROP VIEW IF EXISTS page_count_9;

CREATE VIEW pub_count_9 AS(
  SELECT pub_author.aid, count(*) as pub_num
  FROM pub_author
  JOIN publication USING (pubid)
  WHERE publication.type = 'conf'
  GROUP BY aid
);

CREATE VIEW pub_rank_9 AS(
  SELECT aid, rank() OVER (ORDER BY pub_num DESC)
  FROM pub_count_9
);

SELECT pub_rank_2B.rank, author.name
FROM pub_rank_2B
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

JOIN author
ON collaborator_count.aid = author.aid AND colla_num = (SELECT MAX(colla_num) FROM collaborator_count);
