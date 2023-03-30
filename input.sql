CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255)CHECK (LastName != 0),
    FirstName varchar(255) CHECK (FirstName != LastName),
    Age int CHECK (Age>=18),
    PRIMARY KEY (ID)
);

SELECT * FROM hello;