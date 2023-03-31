DROP TABLE IF EXISTS PERSONS;

CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255) NOT NULL CHECK (LastName IS NOT NULL OR LastName != 'jim' OR LastName != 'wen hao' ),
    FirstName varchar(255) CHECK (FirstName != LastName),
    Age int CHECK (Age>=18),
    PRIMARY KEY (ID)
);
