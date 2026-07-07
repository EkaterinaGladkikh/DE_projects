CREATE TABLE dbo.api_limits
(
    config_key   NVARCHAR(100) NOT NULL
        CONSTRAINT PK_api_limits PRIMARY KEY,

    config_value NVARCHAR(200) NOT NULL
);
GO