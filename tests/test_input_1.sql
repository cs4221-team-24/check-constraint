CREATE TABLE customer (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT CHECK (age >= 18 AND age <= 100),
    email VARCHAR(50) CHECK (email LIKE '%@%.%'),
    city VARCHAR(50) CHECK (credit_limit >= 0),
    country VARCHAR(50) CHECK (
        (city = 'New York' AND country = 'United States') OR
        (city = 'London' AND country = 'United Kingdom') OR
        (city = 'Paris' AND country = 'France') OR
        (city = 'Port Moresby' AND country = 'Papua New Guinea')
    ),
    credit_limit DECIMAL(10,2),
);