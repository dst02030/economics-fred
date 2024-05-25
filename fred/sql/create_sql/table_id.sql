CREATE TABLE IF NOT EXISTS fred.id (
    _ts timestamptz NOT NULL,
    id INT,
    name VARCHAR(255) NOT NULL,
    parent_id INT NOT NULL,
    notes TEXT,
    PRIMARY KEY (id)
);