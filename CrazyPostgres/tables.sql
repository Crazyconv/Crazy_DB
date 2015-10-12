DROP TABLE IF EXISTS author CASCADE;
DROP TABLE IF EXISTS publication CASCADE;
DROP TABLE IF EXISTS article CASCADE;
DROP TABLE IF EXISTS book CASCADE;
DROP TABLE IF EXISTS incollection CASCADE;
DROP TABLE IF EXISTS inproceedings CASCADE;
DROP TABLE IF EXISTS pub_author CASCADE;


CREATE TABLE author (
    aid         SERIAL      PRIMARY KEY,
    name        TEXT        UNIQUE
);

CREATE TABLE publication (
    pubid       SERIAL      PRIMARY KEY,
    pubkey      TEXT        UNIQUE,
    title       TEXT,
    year        INTEGER,
    type        TEXT,
    total_page  INTEGER
);

CREATE TABLE article (
    pubid       INTEGER     PRIMARY KEY REFERENCES publication(pubid),
    journal     TEXT,
    month       INTEGER,
    volume      TEXT,
    number      TEXT
);

CREATE TABLE book (
    pubid       INTEGER     PRIMARY KEY REFERENCES publication(pubid),
    publisher   TEXT,
    isbn        TEXT
);

CREATE TABLE incollection (
    pubid       INTEGER     PRIMARY KEY REFERENCES publication(pubid),
    booktitle   TEXT,
    publisher   TEXT,
    isbn        TEXT
);

CREATE TABLE inproceedings (
    pubid       INTEGER     PRIMARY KEY REFERENCES publication(pubid),
    booktitle   TEXT,
    editor      TEXT
);

CREATE TABLE pub_author (
    pubid       INTEGER     REFERENCES publication(pubid),
    aid         INTEGER     REFERENCES author(aid),
    PRIMARY KEY (pubid, aid)
);
