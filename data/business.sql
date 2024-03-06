-- for use with milestone1DB.csv

CREATE TABLE business (
    name VARCHAR(64),
    state CHAR(2),
    city VARCHAR(32),
    PRIMARY KEY(name, state, city)
);
