CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255)CHECK (LastName != 'fazil'),
    FirstName varchar(255) CHECK (FirstName != LastName),
    Age int CHECK (Age>=18),
    PRIMARY KEY (ID)
);

SELECT * FROM hello;