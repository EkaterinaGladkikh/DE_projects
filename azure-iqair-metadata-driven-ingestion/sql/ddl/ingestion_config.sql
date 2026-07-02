CREATE TABLE dbo.ingestion_config
(
    config_key   NVARCHAR(100) NOT NULL
        CONSTRAINT PK_ingestion_config PRIMARY KEY,
    config_value NVARCHAR(200) NOT NULL
);
GO