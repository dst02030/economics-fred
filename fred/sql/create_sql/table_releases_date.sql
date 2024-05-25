CREATE TABLE IF NOT EXISTS fred.releases_date (
    _ts timestamptz NOT NULL,
    release_id INT,
    release_name VARCHAR(255) NOT NULL,
    release_last_updated timestamptz,
    date DATE NOT NULL,
    PRIMARY KEY (release_id, date)
);