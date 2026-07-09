CREATE OR ALTER PROCEDURE dbo.get_execution_context
(
    @environment NVARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @execution_id NVARCHAR(100);
    DECLARE @status       NVARCHAR(50);

    SELECT TOP (1)
        @execution_id = execution_id,
        @status       = status
    FROM dbo.pipeline_audit
    WHERE record_type = 'PIPELINE'
      AND environment = @environment
    ORDER BY start_ts DESC;

    -- CASE 1: first run OR last run SUCCESS
    IF (@execution_id IS NULL OR @status = 'SUCCESS')
    BEGIN
        SET @execution_id = CONCAT(
            'execution_',
            @environment,
            '_',
            REPLACE(
                CONVERT(CHAR(8), SYSUTCDATETIME(), 112) + '_' + 
                CONVERT(CHAR(8), SYSUTCDATETIME(), 108),
                ':',
                ''
            )
        );
    END
    -- CASE 2: FAILED/SKIPPED -> reuse
    ELSE
    BEGIN    
        SET @execution_id = @execution_id;
    END;

    SELECT @execution_id AS execution_id;
END;
GO