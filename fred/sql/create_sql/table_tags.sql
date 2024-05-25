CREATE TABLE IF NOT EXISTS fred.tags (
    _ts timestamptz NOT NULL,
    name VARCHAR(255) NOT NULL,
    group_id VARCHAR(255) NOT NULL,
    notes TEXT,
    created timestamptz NOT NULL,
    popularity INT NOT NULL,
    series_count INT NOT NULL,
    PRIMARY KEY (name)
);

CREATE INDEX IF NOT EXISTS tags_idx1 ON fred.tags (name, series_count);