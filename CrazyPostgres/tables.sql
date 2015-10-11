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
    volume      INTEGER,
    number      INTEGER
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
