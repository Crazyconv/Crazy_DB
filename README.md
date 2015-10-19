# NTU CZ4031 Lab 1

|          | CZ4031 Lab 1 |
|----------|--------------|
| Group ID | 26           |
| Authors  | Guo Jiachun, Xu Mengxing, Zhou Xinzi |

## What we use?

* PostgreSQL 9.3.9 - DBMS
* PhpPgAdmin       - Browse DB
* Python 2.7       - Extract data and run query
* LXML             - XML Parser
* Pg8000           - Python PostgreSQL Library

## What's inside?

* CrazyExtractor - Python scripts to extract DBLP's xml.
* CrazyPostgres  - SQL scripts to create tables, indexes and query data.
  - tables.sql               - Create tables.
  - index.sql                - Create indexes.
  - query.sql                - Queries.
  - query_indexed.sql        - Queries with index creation.
  - testdata.sql             - Testing data.
  - index_experiment.sql     - Queries to test index's performance.
  - index_explain.sql        - Explain Queries.
* CrazyQuery     - Python scripts to run query.
* CrazyReport    - Final report.
* CrazyResult    - Query time records in TXT format.
* CrazyUtil      - Python scripts to divide an SQL files into half.
