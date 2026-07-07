CREATE OR ALTER PROCEDURE dbo.check_api_limits
(
    @requested_calls INT,
    @bronze_enabled  BIT
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @bronze_enabled = 0
    BEGIN
        RETURN;
    END;

    DECLARE @now         DATETIME2 = SYSDATETIME();
    DECLARE @day_calls   INT;
    DECLARE @max_day     INT;
    DECLARE @max_month   INT;
    DECLARE @month_calls INT;

    SELECT @max_day = CAST(config_value AS INT)
    FROM dbo.api_limits
    WHERE config_key = 'max_calls_per_day';

    SELECT @max_month = CAST(config_value AS INT)
    FROM dbo.api_limits
    WHERE config_key = 'max_calls_per_month';

    IF (@max_day IS NULL OR @max_month IS NULL)
    BEGIN
        THROW 50003, 'API limits are not configured (max_calls_per_day / max_calls_per_month missing in api_limits).', 1;
    END;

    SELECT @day_calls = COUNT(*)
    FROM dbo.api_call_registry
    WHERE CAST(call_ts AS DATE) = CAST(@now AS DATE);

    SELECT @month_calls = COUNT(*)
    FROM dbo.api_call_registry
    WHERE YEAR(call_ts)  = YEAR(@now)
      AND MONTH(call_ts) = MONTH(@now);

    IF (@day_calls + @requested_calls > @max_day)
    BEGIN
        THROW 50001, 'Daily API limit would be exceeded by this pipeline execution.', 1;
    END;

    IF (@month_calls + @requested_calls > @max_month)
    BEGIN
        THROW 50002, 'Monthly API limit would be exceeded by this pipeline execution.', 1;
    END;
END;
GO