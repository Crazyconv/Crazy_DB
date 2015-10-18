-- single index

/*********************** pub_author.aid ************************/
CREATE INDEX pub_author_aid_index ON pub_author (aid);
ANALYZE;

----------
-- Query 2A
-- pub_author.aid: GROUP BY
DROP VIEW IF EXISTS pub_count_2A CASCADE;
DROP VIEW IF EXISTS pub_rank_2A CASCADE;
CREATE VIEW pub_count_2A AS(
  SELECT aid, count(*) AS num_pub
  FROM pub_author
  GROUP BY aid
);

CREATE VIEW pub_rank_2A AS(
  SELECT aid, rank() OVER (ORDER BY num_pub DESC)
  FROM pub_count_2A
);

EXPLAIN SELECT pub_rank_2A.rank, author.name
FROM pub_rank_2A
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;


----------
-- Query 2B
-- pub_author.aid: GROUP BY
DROP VIEW IF EXISTS pub_count_2B CASCADE;
DROP VIEW IF EXISTS pub_rank_2B CASCADE;
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

EXPLAIN SELECT pub_rank_2B.rank, author.name
FROM pub_rank_2B
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
-- Query 3 : Author: Yan Zhang
-- Query 3A
-- pub_author.aid: JOIN

DROP VIEW IF EXISTS pub_info_3A CASCADE;
CREATE VIEW pub_info_3A AS(
  SELECT author.name, publication.*
  FROM pub_author
  JOIN author ON pub_author.aid = author.aid
  JOIN publication ON pub_author.pubid = publication.pubid
  WHERE author.name = 'Yan Zhang' and publication.year = 2012
);

EXPLAIN SELECT * FROM pub_info_3A
LEFT JOIN article ON pub_info_3A.pubid = article.pubid
LEFT JOIN book ON pub_info_3A.pubid = book.pubid
LEFT JOIN incollection ON pub_info_3A.pubid = incollection.pubid
LEFT JOIN inproceedings ON pub_info_3A.pubid = inproceedings.pubid;


----------
-- Query 3B
-- pub_author.aid: JOIN

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';


----------
--Query 4A:
-- pub_author.aid: GROUP BY

DROP VIEW IF EXISTS PVLDB_4 CASCADE;
DROP VIEW IF EXISTS KDD_4A CASCADE;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
  HAVING count(aid) >= 10
);

CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
DROP VIEW IF EXISTS P10K5 CASCADE;
CREATE VIEW P10K5 AS(
  SELECT aid FROM PVLDB_4
  INTERSECT
  SELECT aid FROM KDD_4A WHERE KDD_num >= 5
);
EXPLAIN SELECT name
FROM author JOIN P10K5 ON (author.aid = P10K5.aid);

----------
--Query 4B:
DROP VIEW IF EXISTS P10K0 CASCADE;
CREATE VIEW P10K0 AS(
  SELECT aid FROM PVLDB_4
  EXCEPT
  SELECT aid FROM KDD_4A
);
EXPLAIN SELECT name
FROM author JOIN P10K0 ON (author.aid = P10K0.aid);

----------
--Query 5 create view only
DROP VIEW IF EXISTS decade_1970 CASCADE;
DROP VIEW IF EXISTS decade_1980 CASCADE;
DROP VIEW IF EXISTS decade_1990 CASCADE;
DROP VIEW IF EXISTS decade_2000 CASCADE;
DROP VIEW IF EXISTS decade_2010 CASCADE;

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

----------
-- Query 6:
-- pub_author.aid: GROUP BY
DROP VIEW IF EXISTS decade_1970_top_author CASCADE;
DROP VIEW IF EXISTS decade_1980_top_author CASCADE;
DROP VIEW IF EXISTS decade_1990_top_author CASCADE; 
DROP VIEW IF EXISTS decade_2000_top_author CASCADE;
DROP VIEW IF EXISTS decade_2010_top_author CASCADE;


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

EXPLAIN 
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
-- pub_author.aid: JOIN
DROP VIEW IF EXISTS collaborator CASCADE;
DROP VIEW IF EXISTS collaborator_counts CASCADE;


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

EXPLAIN SELECT author.name
FROM collaborator_count
JOIN author
ON collaborator_count.aid = author.aid AND colla_num = (SELECT MAX(colla_num) FROM collaborator_count);


----------
-- Query 8
-- pub_author.aid: GROUP BY
DROP VIEW IF EXISTS page_count_8 CASCADE;

CREATE VIEW page_count_8 AS(
  SELECT pub_author.aid, SUM(total_page) AS total_page
  FROM pub_author
  JOIN publication USING (pubid)
  GROUP BY aid
);

EXPLAIN SELECT author.name, total_page
FROM page_count_8
JOIN author USING (aid)
WHERE total_page >= 4000
ORDER by total_page DESC;


----------
-- Query 9
-- pub_author.aid: GROUP BY
DROP VIEW IF EXISTS pub_count_9 CASCADE;
DROP VIEW IF EXISTS pub_rank_9 CASCADE;

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

EXPLAIN SELECT pub_rank_9.rank, author.name
FROM pub_rank_9
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
DROP INDEX IF EXISTS pub_author_aid_index;


/*********************** pub_author.pubid ************************/
CREATE INDEX pub_author_pubid_index ON pub_author (pubid);
ANALYZE;

----------
-- Query 2B
-- pub_author.pubid: JOIN
DROP VIEW IF EXISTS pub_count_2B CASCADE;
DROP VIEW IF EXISTS pub_rank_2B CASCADE;
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

EXPLAIN SELECT pub_rank_2B.rank, author.name
FROM pub_rank_2B
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
-- Query 3 : Author: Yan Zhang
-- Query 3A
-- pub_author.pubid: JOIN

DROP VIEW IF EXISTS pub_info_3A CASCADE;
CREATE VIEW pub_info_3A AS(
  SELECT author.name, publication.*
  FROM pub_author
  JOIN author ON pub_author.aid = author.aid
  JOIN publication ON pub_author.pubid = publication.pubid
  WHERE author.name = 'Yan Zhang' and publication.year = 2012
);

EXPLAIN SELECT * FROM pub_info_3A
LEFT JOIN article ON pub_info_3A.pubid = article.pubid
LEFT JOIN book ON pub_info_3A.pubid = book.pubid
LEFT JOIN incollection ON pub_info_3A.pubid = incollection.pubid
LEFT JOIN inproceedings ON pub_info_3A.pubid = inproceedings.pubid;


----------
-- Query 3B
-- pub_author.pubid: JOIN

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';


----------
--Query 4A:
-- pub_author.pubid: JOIN

DROP VIEW IF EXISTS PVLDB_4 CASCADE;
DROP VIEW IF EXISTS KDD_4A CASCADE;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
  HAVING count(aid) >= 10
);

CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
DROP VIEW IF EXISTS P10K5 CASCADE;
CREATE VIEW P10K5 AS(
  SELECT aid FROM PVLDB_4
  INTERSECT
  SELECT aid FROM KDD_4A WHERE KDD_num >= 5
);
EXPLAIN SELECT name
FROM author JOIN P10K5 ON (author.aid = P10K5.aid);

----------
--Query 4B:
DROP VIEW IF EXISTS P10K0 CASCADE;
CREATE VIEW P10K0 AS(
  SELECT aid FROM PVLDB_4
  EXCEPT
  SELECT aid FROM KDD_4A
);
EXPLAIN SELECT name
FROM author JOIN P10K0 ON (author.aid = P10K0.aid);

----------
--Query 5 create view only
DROP VIEW IF EXISTS decade_1970 CASCADE;
DROP VIEW IF EXISTS decade_1980 CASCADE;
DROP VIEW IF EXISTS decade_1990 CASCADE;
DROP VIEW IF EXISTS decade_2000 CASCADE;
DROP VIEW IF EXISTS decade_2010 CASCADE;

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

----------
-- Query 6:
-- pub_author.pubid: JOIN
DROP VIEW IF EXISTS decade_1970_top_author CASCADE;
DROP VIEW IF EXISTS decade_1980_top_author CASCADE;
DROP VIEW IF EXISTS decade_1990_top_author CASCADE; 
DROP VIEW IF EXISTS decade_2000_top_author CASCADE;
DROP VIEW IF EXISTS decade_2010_top_author CASCADE;


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
EXPLAIN 
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
-- pub_author.pubid: JOIN
DROP VIEW IF EXISTS collaborator CASCADE;
DROP VIEW IF EXISTS collaborator_counts CASCADE;


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

EXPLAIN SELECT author.name
FROM collaborator_count
JOIN author
ON collaborator_count.aid = author.aid AND colla_num = (SELECT MAX(colla_num) FROM collaborator_count);


----------
-- Query 8
-- select the authors who have writen more than 4000 pages of publication
-- pub_author.pubid: JOIN
DROP VIEW IF EXISTS page_count_8 CASCADE;

CREATE VIEW page_count_8 AS(
  SELECT pub_author.aid, SUM(total_page) AS total_page
  FROM pub_author
  JOIN publication USING (pubid)
  GROUP BY aid
);

EXPLAIN SELECT author.name, total_page
FROM page_count_8
JOIN author USING (aid)
WHERE total_page >= 4000
ORDER by total_page DESC;


----------
-- Query 9
-- pub_author.aid
-- pub_author.pubid: JOIN

DROP VIEW IF EXISTS pub_count_9 CASCADE;
DROP VIEW IF EXISTS pub_rank_9 CASCADE;

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

EXPLAIN SELECT pub_rank_9.rank, author.name
FROM pub_rank_9
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;


----------
DROP INDEX IF EXISTS pub_author_pubid_index;

/*********************** publication.year ************************/
CREATE INDEX publication_year_index ON publication (year);
ANALYZE;

----------
-- Query 3 : Author: Yan Zhang
-- Query 3A
-- publication.year: WHERE

DROP VIEW IF EXISTS pub_info_3A CASCADE;
CREATE VIEW pub_info_3A AS(
  SELECT author.name, publication.*
  FROM pub_author
  JOIN author ON pub_author.aid = author.aid
  JOIN publication ON pub_author.pubid = publication.pubid
  WHERE author.name = 'Yan Zhang' and publication.year = 2012
);

EXPLAIN SELECT * FROM pub_info_3A
LEFT JOIN article ON pub_info_3A.pubid = article.pubid
LEFT JOIN book ON pub_info_3A.pubid = book.pubid
LEFT JOIN incollection ON pub_info_3A.pubid = incollection.pubid
LEFT JOIN inproceedings ON pub_info_3A.pubid = inproceedings.pubid;

----------
-- Query 3B
-- publication.year: WHERE

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';


----------
--Query 5:
-- publication.year: WHERE
DROP VIEW IF EXISTS decade_1970 CASCADE;
DROP VIEW IF EXISTS decade_1980 CASCADE;
DROP VIEW IF EXISTS decade_1990 CASCADE;
DROP VIEW IF EXISTS decade_2000 CASCADE;
DROP VIEW IF EXISTS decade_2010 CASCADE;

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
EXPLAIN 
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
DROP INDEX IF EXISTS publication_year_index;


/*********************** publication.type ************************/
CREATE INDEX publication_type_index ON publication (type);
ANALYZE;

----------
-- Query 3B
-- publication.type: WHERE

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';


----------
-- Query 9
-- publication.type: WHERE

DROP VIEW IF EXISTS pub_count_9 CASCADE;
DROP VIEW IF EXISTS pub_rank_9 CASCADE;

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

EXPLAIN SELECT pub_rank_9.rank, author.name
FROM pub_rank_9
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
DROP INDEX IF EXISTS publication_type_index;


/*********************** article.journal ************************/
CREATE INDEX article_journal_index ON article (journal);
ANALYZE;

----------
-- Query 3B
-- article.journal: WHERE

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';


----------
--Query 4A:
-- article.journal: WHERE

DROP VIEW IF EXISTS PVLDB_4 CASCADE;
DROP VIEW IF EXISTS KDD_4A CASCADE;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
  HAVING count(aid) >= 10
);

CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
DROP VIEW IF EXISTS P10K5 CASCADE;
CREATE VIEW P10K5 AS(
  SELECT aid FROM PVLDB_4
  INTERSECT
  SELECT aid FROM KDD_4A WHERE KDD_num >= 5
);
EXPLAIN SELECT name
FROM author JOIN P10K5 ON (author.aid = P10K5.aid);

----------
--Query 4B:
DROP VIEW IF EXISTS P10K0 CASCADE;
CREATE VIEW P10K0 AS(
  SELECT aid FROM PVLDB_4
  EXCEPT
  SELECT aid FROM KDD_4A
);
EXPLAIN SELECT name
FROM author JOIN P10K0 ON (author.aid = P10K0.aid);


----------
DROP INDEX IF EXISTS article_journal_index;


/*********************** inproceedings.booktitle ************************/
CREATE INDEX inproceedings_booktitle_index ON inproceedings (booktitle);
ANALYZE;

----------
-- Query 3B
-- inproceedings.booktitle: WHERE

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';


----------
--Query 4A:
-- inproceedings.booktitle: WHERE

DROP VIEW IF EXISTS PVLDB_4 CASCADE;
DROP VIEW IF EXISTS KDD_4A CASCADE;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
  HAVING count(aid) >= 10
);

CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
DROP VIEW IF EXISTS P10K5 CASCADE;
CREATE VIEW P10K5 AS(
  SELECT aid FROM PVLDB_4
  INTERSECT
  SELECT aid FROM KDD_4A WHERE KDD_num >= 5
);
EXPLAIN SELECT name
FROM author JOIN P10K5 ON (author.aid = P10K5.aid);

----------
--Query 4B:
DROP VIEW IF EXISTS P10K0 CASCADE;
CREATE VIEW P10K0 AS(
  SELECT aid FROM PVLDB_4
  EXCEPT
  SELECT aid FROM KDD_4A
);
EXPLAIN SELECT name
FROM author JOIN P10K0 ON (author.aid = P10K0.aid);

----------
DROP INDEX IF EXISTS inproceedings_booktitle_index;



-- full index
/*********************** pub_author.aid ************************/
/*********************** pub_author.pubid ************************/

CREATE INDEX pub_author_aid_index ON pub_author (aid);
CREATE INDEX pub_author_pubid_index ON pub_author (pubid);
ANALYZE;

----------
-- Query 2B


DROP VIEW IF EXISTS pub_count_2B CASCADE;
DROP VIEW IF EXISTS pub_rank_2B CASCADE;
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

EXPLAIN SELECT pub_rank_2B.rank, author.name
FROM pub_rank_2B
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
--Query 5 create view only
DROP VIEW IF EXISTS decade_1970 CASCADE;
DROP VIEW IF EXISTS decade_1980 CASCADE;
DROP VIEW IF EXISTS decade_1990 CASCADE;
DROP VIEW IF EXISTS decade_2000 CASCADE;
DROP VIEW IF EXISTS decade_2010 CASCADE;

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

----------
-- Query 6:
-- pub_author.aid
-- pub_author.pubid
DROP VIEW IF EXISTS decade_1970_top_author CASCADE;
DROP VIEW IF EXISTS decade_1980_top_author CASCADE;
DROP VIEW IF EXISTS decade_1990_top_author CASCADE; 
DROP VIEW IF EXISTS decade_2000_top_author CASCADE;
DROP VIEW IF EXISTS decade_2010_top_author CASCADE;


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
EXPLAIN 
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
-- pub_author.aid
-- pub_author.pubid
DROP VIEW IF EXISTS collaborator CASCADE;
DROP VIEW IF EXISTS collaborator_counts CASCADE;


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

EXPLAIN SELECT author.name
FROM collaborator_count
JOIN author
ON collaborator_count.aid = author.aid AND colla_num = (SELECT MAX(colla_num) FROM collaborator_count);


----------
-- Query 8
-- pub_author.pubid
-- pub_author.aid

DROP VIEW IF EXISTS page_count_8 CASCADE;

CREATE VIEW page_count_8 AS(
  SELECT pub_author.aid, SUM(total_page) AS total_page
  FROM pub_author
  JOIN publication USING (pubid)
  GROUP BY aid
);

EXPLAIN SELECT author.name, total_page
FROM page_count_8
JOIN author USING (aid)
WHERE total_page >= 4000
ORDER by total_page DESC;




----------
/*********************** pub_author.aid ************************/
/*********************** pub_author.pubid ************************/
/*********************** publication.year ************************/

CREATE INDEX publication_year_index ON publication (year);
ANALYZE;

----------
-- Query 3 : Author: Yan Zhang
-- Query 3A
-- pub_author.aid
-- pub_author.pubid
-- author.aid -> already index
-- author.name -> already index
-- publication.year
-- publication/....pubid -> already index

DROP VIEW IF EXISTS pub_info_3A CASCADE;
CREATE VIEW pub_info_3A AS(
  SELECT author.name, publication.*
  FROM pub_author
  JOIN author ON pub_author.aid = author.aid
  JOIN publication ON pub_author.pubid = publication.pubid
  WHERE author.name = 'Yan Zhang' and publication.year = 2012
);

EXPLAIN SELECT * FROM pub_info_3A
LEFT JOIN article ON pub_info_3A.pubid = article.pubid
LEFT JOIN book ON pub_info_3A.pubid = book.pubid
LEFT JOIN incollection ON pub_info_3A.pubid = incollection.pubid
LEFT JOIN inproceedings ON pub_info_3A.pubid = inproceedings.pubid;


----------
/*********************** pub_author.aid ************************/
/*********************** pub_author.pubid ************************/
/*********************** publication.year ************************/
/*********************** inproceedings.booktitle ************************/
/*********************** article.journal ************************/
/*********************** publication.type ************************/

CREATE INDEX publication_type_index ON publication (type);
CREATE INDEX article_journal_index ON article (journal);
CREATE INDEX inproceedings_booktitle_index ON inproceedings (booktitle);
ANALYZE;


----------
-- Query 3B

DROP VIEW IF EXISTS pub_info_3B CASCADE;
CREATE VIEW pub_info_3B AS(
  SELECT author.name, publication.*
  FROM author 
  JOIN pub_author ON (author.aid = pub_author.aid)
  JOIN publication ON (pub_author.pubid = publication.pubid)
  WHERE author.name = 'Wei Wang' AND year = 2009 AND type = 'conf'
);

EXPLAIN SELECT * FROM pub_info_3B
JOIN article USING (pubid)
WHERE article.journal = 'CSCWD';

EXPLAIN SELECT * FROM pub_info_3B
JOIN inproceedings USING (pubid)
WHERE inproceedings.booktitle = 'CSCWD';

----------
/*********************** pub_author.aid ************************/
/*********************** pub_author.pubid ************************/
/*********************** inproceedings.booktitle ************************/
/*********************** article.journal ************************/

DROP INDEX IF EXISTS publication_year_index;
DROP INDEX IF EXISTS publication_type_index;
ANALYZE;

----------
--Query 4A:
-- pub_author.aid
-- pub_author.pubid
-- article.pubid -> already index
-- article.journal
-- inproceedings.pubid -> already index
-- inproceedings.booktitle
-- index on author.aid -> already index

DROP VIEW IF EXISTS PVLDB_4 CASCADE;
DROP VIEW IF EXISTS KDD_4A CASCADE;
CREATE VIEW PVLDB_4 AS(
  SELECT pub_author.aid, count(*) AS PVLDB_num
  FROM pub_author
  JOIN article ON pub_author.pubid = article.pubid
  WHERE article.journal = 'PVLDB'
  GROUP BY aid
  HAVING count(aid) >= 10
);

CREATE VIEW KDD_4A AS(
  SELECT pub_author.aid, count(*) AS KDD_num
  FROM pub_author
  JOIN inproceedings ON pub_author.pubid = inproceedings.pubid
  WHERE inproceedings.booktitle = 'KDD'
  GROUP BY aid
);

-- Query 4A:
DROP VIEW IF EXISTS P10K5 CASCADE;
CREATE VIEW P10K5 AS(
  SELECT aid FROM PVLDB_4
  INTERSECT
  SELECT aid FROM KDD_4A WHERE KDD_num >= 5
);
EXPLAIN SELECT name
FROM author JOIN P10K5 ON (author.aid = P10K5.aid);

----------
--Query 4B:
DROP VIEW IF EXISTS P10K0 CASCADE;
CREATE VIEW P10K0 AS(
  SELECT aid FROM PVLDB_4
  EXCEPT
  SELECT aid FROM KDD_4A
);
EXPLAIN SELECT name
FROM author JOIN P10K0 ON (author.aid = P10K0.aid);


----------
/*********************** pub_author.aid ************************/
/*********************** pub_author.pubid ************************/
/*********************** publication.type ************************/

DROP INDEX IF EXISTS article_journal_index;
DROP INDEX IF EXISTS inproceedings_booktitle_index;
CREATE INDEX publication_type_index ON publication (type);
ANALYZE;

----------
-- Query 9
-- pub_author.aid
-- pub_author.pubid
-- publication.type
DROP VIEW IF EXISTS pub_count_9 CASCADE;
DROP VIEW IF EXISTS pub_rank_9 CASCADE;

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

EXPLAIN SELECT pub_rank_9.rank, author.name
FROM pub_rank_9
JOIN author USING (aid)
WHERE rank <= 10
ORDER BY rank;

----------
/*********************** publication.year ************************/

DROP INDEX IF EXISTS pub_author_aid_index;
DROP INDEX IF EXISTS pub_author_pubid_index;
DROP INDEX IF EXISTS publication_type_index;
CREATE INDEX publication_year_index ON publication (year);
ANALYZE;

----------
--Query 5:
-- publication.year
DROP VIEW IF EXISTS decade_1970 CASCADE;
DROP VIEW IF EXISTS decade_1980 CASCADE;
DROP VIEW IF EXISTS decade_1990 CASCADE;
DROP VIEW IF EXISTS decade_2000 CASCADE;
DROP VIEW IF EXISTS decade_2010 CASCADE;

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
EXPLAIN 
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
DROP INDEX IF EXISTS publication_year_index;