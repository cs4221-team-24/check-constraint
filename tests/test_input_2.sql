CREATE TABLE customer (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    email VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50),
    credit_limit DECIMAL(10,2),
    CONSTRAINT age_check CHECK (age >= 18 AND age <= 100),
    CONSTRAINT email_check CHECK (email LIKE '%@%.%'),
    CONSTRAINT credit_check CHECK (credit_limit >= 0),
    CONSTRAINT city_country_check CHECK (
        (city = 'New York' AND country = 'United States') OR
        (city = 'London' AND country = 'United Kingdom') OR
        (city = 'Paris' AND country = 'France') OR
        (city = 'Port Moresby' AND country = 'Papua New Guinea')
    )
);