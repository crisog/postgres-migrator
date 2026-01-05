-- Public schema tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO users (name) VALUES ('Alice'), ('Bob');

-- Excluded schema
CREATE SCHEMA excluded_schema;

CREATE TABLE excluded_schema.internal_data (
    id SERIAL PRIMARY KEY,
    value TEXT
);

INSERT INTO excluded_schema.internal_data (value) VALUES ('secret1'), ('secret2');
