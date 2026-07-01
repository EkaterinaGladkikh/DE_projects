CREATE TABLE dbo.pipeline_audit
(
    audit_id        BIGINT IDENTITY(1,1)
        CONSTRAINT PK_pipeline_audit PRIMARY KEY,
    record_type     NVARCHAR(20)  NOT NULL,
    execution_id    NVARCHAR(100) NOT NULL,
    pipeline_run_id NVARCHAR(100) NOT NULL,
    environment     NVARCHAR(50)  NOT NULL,
    activity_name   NVARCHAR(200) NULL,
    entity          NVARCHAR(100) NULL,
    layer           NVARCHAR(20)  NULL,
    status          NVARCHAR(20)  NOT NULL,
    start_ts        DATETIME2     NOT NULL,
    end_ts          DATETIME2     NULL,
    error_message   NVARCHAR(MAX) NULL,
    CONSTRAINT CK_pipeline_audit_record_type
        CHECK (record_type IN ('PIPELINE', 'ACTIVITY')),
    CONSTRAINT CK_pipeline_audit_status
        CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED', 'SKIPPED'))
);
GO