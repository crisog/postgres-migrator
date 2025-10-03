CREATE TABLE random_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INT,
    salary DECIMAL(10, 2),
    created_at TIMESTAMP
);

INSERT INTO random_data (name, email, age, salary, created_at)
SELECT
    'User_' || i,
    'user' || i || '@example.com',
    (random() * 60 + 18)::INT,
    (random() * 150000 + 30000)::DECIMAL(10,2),
    NOW() - (random() * INTERVAL '365 days')
FROM generate_series(1, 1000000) AS i;
