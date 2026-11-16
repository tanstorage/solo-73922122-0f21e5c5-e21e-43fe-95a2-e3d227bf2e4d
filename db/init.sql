-- Source schema for the offline-QC replication demo.
-- Contract: row-level INSERT/UPDATE/DELETE only; all columns non-null integers;
-- at most 500 rows per table.

CREATE TABLE stations (
    id         integer PRIMARY KEY,
    correction integer NOT NULL
);

CREATE TABLE readings (
    id         integer PRIMARY KEY,
    station_id integer NOT NULL,
    value      integer NOT NULL
);

INSERT INTO stations
SELECT g, ((g * 7) % 101) - 50
FROM generate_series(1, 200) AS g;

INSERT INTO readings
SELECT g, 1 + (g % 200), ((g * 13) % 1001) - 500
FROM generate_series(1, 500) AS g;
