CREATE OR ALTER PROCEDURE dbo.end_pipeline_run
(
    @execution_id    NVARCHAR(100),
    @pipeline_run_id NVARCHAR(100),
    @final_status    NVARCHAR(20),
    @error_message   NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @final_status NOT IN ('SUCCESS', 'FAILED', 'SKIPPED')
    BEGIN
        THROW 50001, 'Invalid final status.', 1;
    END;

    UPDATE dbo.pipeline_audit
    SET status        = @final_status,
        end_ts        = SYSDATETIME(),
        error_message = @error_message
    WHERE record_type     = 'PIPELINE'
      AND execution_id    = @execution_id
      AND pipeline_run_id = @pipeline_run_id
      AND end_ts IS NULL;
END;
GO