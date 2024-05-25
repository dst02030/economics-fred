CREATE TABLE IF NOT EXISTS fred.releases (
    _ts timestamptz NOT NULL,
    id INT,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    name VARCHAR(255) NOT NULL,
    press_release BOOLEAN NOT NULL,
    link VARCHAR(255),
    notes TEXT,
    PRIMARY KEY (id, realtime_end)
);

CREATE INDEX releases_idx1 ON red.releases (id, realtime_start, realtime_end);