-- Query 1

(SELECT 'Article' AS type, count(*) AS num FROM article)
UNION
(SELECT 'Book' AS type, count(*) AS num FROM book)
UNION
(SELECT 'Incollection' AS type, count(*) AS num FROM incollection)
UNION
(SELECT 'Inproceedings' AS type, count(*) AS num FROM inproceedings);

----------
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
