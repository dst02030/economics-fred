CREATE TABLE IF NOT EXISTS fred.observations (
    _ts timestamptz NOT NULL,
    id VARCHAR(255) NOT NULL,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(24, 4),
    PRIMARY KEY (id, date)
);