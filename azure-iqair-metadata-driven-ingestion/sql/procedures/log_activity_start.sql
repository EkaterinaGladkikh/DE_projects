CREATE OR ALTER PROCEDURE dbo.log_activity_start
(
    @execution_id    NVARCHAR(100),
    @pipeline_run_id NVARCHAR(100),
    @environment     NVARCHAR(50),
    @activity_name   NVARCHAR(200),
    @entity          NVARCHAR(100) = NULL,
    @state           NVARCHAR(100) = NULL,
    @country         NVARCHAR(100) = NULL,
    @layer           NVARCHAR(20) = NULL
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
        activity_name,
        entity,
        state,
        country,
        layer,
        status,
        start_ts
    )
    VALUES
    (
        'ACTIVITY',
        @execution_id,
        @pipeline_run_id,
        @environment,
        @activity_name,
        @entity,
        @state,
        @country,
        @layer,
        'STARTED',
        SYSDATETIME()
    );
END;
GO