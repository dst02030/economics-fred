CREATE TABLE IF NOT EXISTS fred.sources (
    _ts timestamptz NOT NULL,
    id INT,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    name VARCHAR(255) NOT NULL,
    link VARCHAR(255),
    notes TEXT,
    PRIMARY KEY (id, realtime_end)
);

CREATE INDEX sources_idx1 ON fred.sources (id, realtime_start, realtime_end);