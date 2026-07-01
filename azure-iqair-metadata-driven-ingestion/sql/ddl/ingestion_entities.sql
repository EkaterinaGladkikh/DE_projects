CREATE TABLE dbo.ingestion_entities
(
    entity_id INT IDENTITY(1,1)
        CONSTRAINT PK_ingestion_entities PRIMARY KEY,
    city      NVARCHAR(100) NOT NULL,
    state     NVARCHAR(100) NOT NULL,
    country   NVARCHAR(100) NOT NULL,
    is_active BIT NOT NULL
        CONSTRAINT DF_ingestion_entities_is_active DEFAULT (1)
);
GO