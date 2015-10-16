-- Query 1

(SELECT 'Article' AS type, count(*) AS num FROM article)
UNION
(SELECT 'Book' AS type, count(*) AS num FROM book)
UNION
(SELECT 'Incollection' AS type, count(*) AS num FROM incollection)
UNION
(SELECT 'Inproceeding' AS type, count(*) AS num FROM inproceedings);

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

DROP VIEW IF EXISTS KDD_4A;
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
DROP VIEW IF EXISTS decade_author;
DROP VIEW IF EXISTS decade_pub;

CREATE VIEW decade_author AS(
  SELECT pub_author.*, publication.year
  FROM pub_author
  LEFT JOIN publication USING (pubid)
);

CREATE VIEW decade_pub AS(
  (SELECT '1970-1979' AS decade, aid, count(pubid) as pub_num
  FROM decade_author
  WHERE year >= 1970 and year <= 1979
  GROUP BY aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '1980-1989' AS decade, aid, count(pubid) as pub_num
  FROM decade_author
  WHERE year >= 1980 and year <= 1989
  GROUP BY aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '1990-1999' AS decade, aid, count(pubid) as pub_num
  FROM decade_author
  WHERE year >= 1990 and year <= 1999
  GROUP BY aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '2000-2009' AS decade, aid, count(pubid) as pub_num
  FROM decade_author
  WHERE year >= 2000 and year <= 2009
  GROUP BY aid
  ORDER BY pub_num DESC
  LIMIT 1)
  UNION
  (SELECT '2010_2019' AS decade, aid, count(pubid) as pub_num
  FROM decade_author
  WHERE year >= 2010 and year <= 2019
  GROUP BY aid
  ORDER BY pub_num DESC
  LIMIT 1)
);

SELECT decade_pub.decade, author.name
FROM decade_pub
LEFT JOIN author ON decade_pub.aid = author.aid;

--Query 7
DROP VIEW IF EXISTS collaborator;
DROP VIEW IF EXISTS collaborator_counts;


CREATE VIEW collaborator AS(
  SELECT a.*, b.aid as colla_id
  FROM pub_author a
  JOIN pub_author b ON a.pubid = b.pubid and NOT a.aid = b.aid
);

CREATE VIEW collaborator_count AS(
  SELECT aid, count(DISTINCT colla_id) AS colla_num
  FROM collaborator
  GROUP BY aid
  ORDER BY colla_num DESC
  LIMIT 1
);

SELECT author.name
FROM collaborator_count
LEFT JOIN author
ON collaborator_count.aid = author.aid

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
