USE Prime;
GO

CREATE TABLE MyTable(
  Id int NOT NULL PRIMARY KEY,
  valueDate DATETIME,
  valueFloat FLOAT,
  valueString nvarchar(max),
  valueDecimal DECIMAL,
  valueNumber NUMERIC(10,5),
);
GO