CREATE OR ALTER PROCEDURE dbo.check_minute_limit
AS
BEGIN
    SET NOCOUNT ON;
 
    -- SET_EntitiesArray forces an empty entity array when bronze is disabled,
    -- so the loop (and this check) never runs unless bronze is enabled.
 
    DECLARE @now            DATETIME2 = SYSDATETIME();
    DECLARE @max_minute     INT;
    DECLARE @last_minute    INT;
 
    SELECT @max_minute = CAST(config_value AS INT)
    FROM dbo.api_limits
    WHERE config_key = 'max_calls_per_minute';
 
    IF (@max_minute IS NULL)
    BEGIN
        THROW 50004, 'Minute API limit is not configured (max_calls_per_minute missing in api_limits).', 1;
    END;

    SELECT @last_minute = COUNT(*)
    FROM dbo.api_call_registry
    WHERE call_ts > DATEADD(SECOND, -60, @now);
 
    IF (@last_minute + 1 > @max_minute)
    BEGIN
        THROW 50005, 'Per-minute API limit would be exceeded by this Bronze call.', 1;
    END;
END;
GO