-- Query 1

(SELECT 'Article' AS type, count(*) AS num FROM article)
UNION
(SELECT 'Book' AS type, count(*) AS num FROM book)
UNION
(SELECT 'Incollection' AS type, count(*) AS num FROM incollection)
UNION
(SELECT 'Inproceeding' AS type, count(*) AS num FROM inproceeding);

-- Query 2A

DROP VIEW IF EXISTS pub_count_2A;
CREATE VIEW pub_count_2A AS(
  SELECT aid, count(*) AS num_pub
  FROM pub_author
  GROUP BY aid
  ORDER BY num_pub DESC
  LIMIT 10
);

SELECT author.name FROM pub_count_2A LEFT JOIN author ON pub_count_2A.aid = author.aid;

-- Query 2B

DROP VIEW IF EXISTS pub_count_2B;
CREATE VIEW pub_count_2B AS(
  SELECT pub_author.aid, SUM(total_page) AS total_page
  FROM pub_author
  LEFT JOIN publication ON pub_author.pubid = publication.pubid
  GROUP BY aid
  ORDER BY total_page DESC
  LIMIT 10
);

SELECT author.name FROM pub_count_2B LEFT JOIN author ON pub_count_2B.aid = author.aid;

-- Query 3 : Author: Yan Zhang
-- Query 3A

DROP VIEW IF EXISTS pub_info_3A;
CREATE VIEW pub_info_3A AS(
  SELECT author.name, publication.*
  FROM pub_author
  LEFT JOIN author ON pub_author.aid = author.aid
  LEFT JOIN publication ON pub_author.pubid = publication.pubid
  WHERE author.name = 'Yan Zhang' and publication.year = 2012
);

SELECT * FROM pub_info_3A
LEFT JOIN article ON pub_info_3A.pubid = article.pubid
LEFT JOIN book ON pub_info_3A.pubid = book.pubid
LEFT JOIN incollection ON pub_info_3A.pubid = incollection.pubid
LEFT JOIN inproceedings ON pub_info_3A.pubid = inproceedings.pubid;

--Query 4A:

DROP VIEW IF EXISTS PVLDB_4;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  LEFT JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
);

DROP VIEW IF EXISTS KDD_4;
CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  LEFT JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
SELECT author.name
FROM PVLDB_4
INNER JOIN KDD_4A ON PVLDB_4.aid = KDD_4A.aid
LEFT JOIN author ON PVLDB_4.aid = author.aid
Where PVLDB_4.PVLDB_num >= 10 and KDD_4A.KDD_num >= 5;

--Query 4B:
SELECT author.name
FROM PVLDB_4
LEFT JOIN KDD_4A ON PVLDB_4.aid = KDD_4A.aid
LEFT JOIN author ON PVLDB_4.aid = author.aid
Where PVLDB_4.PVLDB_num >= 10 and KDD_4A.KDD_num is NULL;

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

-- Query 6:
DROP VIEW IF EXISTS decade_1970;
DROP VIEW IF EXISTS decade_1980;
DROP VIEW IF EXISTS decade_1990;
DROP VIEW IF EXISTS decade_2000;
DROP VIEW IF EXISTS decade_2010;
DROP VIEW IF EXISTS decade_pub;

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

CREATE VIEW decade_pub AS(
  (SELECT '1970-1979' AS decade, pub_author.aid, count(pubid) as pub_num
  FROM decade_1970
  LEFT JOIN pub_author USING (pubid)
  GROUP BY pub_author.aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '1980-1989' AS decade, pub_author.aid, count(pubid) as pub_num
  FROM decade_1980
  LEFT JOIN pub_author USING (pubid)
  GROUP BY pub_author.aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '1990-1999' AS decade, pub_author.aid, count(pubid) as pub_num
  FROM decade_1990
  LEFT JOIN pub_author USING (pubid)
  GROUP BY pub_author.aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '2000-2009' AS decade, pub_author.aid, count(pubid) as pub_num
  FROM decade_2000
  LEFT JOIN pub_author USING (pubid)
  GROUP BY pub_author.aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '2010_2019' AS decade, pub_author.aid, count(pubid) as pub_num
  FROM decade_author
  LEFT JOIN pub_author USING (pubid)
  GROUP BY pub_author.aid
  ORDER BY pub_num DESC
  LIMIT 1)
);

SELECT decade_pub.decade, author.name
FROM decade_pub
LEFT JOIN author ON decade_pub.aid = author.aid;

--Query 7

CREATE VIEW collaborator AS(
  SELECT a.*, b.aid as colla_id
  FROM pub_author a
  JOIN pub_author b ON a.pubid = b.pubid and NOT a.aid = b.aid
);

CREATE VIEW collaborator_count AS(
  SELECT aid, count(DISTINCT colla_id) AS colla_num
  FROM collaborator
  GROUP BY aid
  ORDER BY colla_num
  LIMIT 1
);

SELECT author.name
FROM collaborator_count
LEFT JOIN author
ON collaborator_count.aid = author.aid
