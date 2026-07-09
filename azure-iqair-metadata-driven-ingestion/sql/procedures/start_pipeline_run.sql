CREATE OR ALTER PROCEDURE dbo.start_pipeline_run
(
    @execution_id    NVARCHAR(100),
    @pipeline_run_id NVARCHAR(100),
    @environment     NVARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO dbo.pipeline_audit
    (
        record_type,
        execution_id,
        pipeline_run_id,
        environment,
        status,
        start_ts
    )
    VALUES
    (
        'PIPELINE',
        @execution_id,
        @pipeline_run_id,
        @environment,
        'STARTED',
        SYSUTCDATETIME()
    );
END;
GO