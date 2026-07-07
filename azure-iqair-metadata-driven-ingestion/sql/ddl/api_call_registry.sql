CREATE TABLE dbo.api_call_registry
(
    api_call_id     BIGINT        IDENTITY(1,1)
        CONSTRAINT PK_api_call_registry PRIMARY KEY,
    
    pipeline_run_id NVARCHAR(100) NOT NULL,
    execution_id    NVARCHAR(100) NOT NULL,
    call_ts         DATETIME2     NOT NULL,
    environment     NVARCHAR(50)  NOT NULL,
    city            NVARCHAR(100) NOT NULL,
    state           NVARCHAR(100) NOT NULL,
    country         NVARCHAR(100) NOT NULL
);
GO