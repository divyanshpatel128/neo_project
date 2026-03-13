
create schema gold;

CREATE OR ALTER PROCEDURE etl.gold_server_monitoring_data
AS
BEGIN
    SET NOCOUNT ON;

    -- Drop target table if it exists
    IF OBJECT_ID('GOLD.ServerMetrics_Business', 'U') IS NOT NULL
        DROP TABLE GOLD.ServerMetrics_Business;

    -- Create gold table with calculated metrics
    SELECT 
        Server_id,
        hostname,
        ip_address,
        os_type,
        TRIM(server_location_city) AS server_location_city,
        TRIM(server_location_country) AS server_location_country,
        CAST(CPU_Utilization AS FLOAT) AS CPU_Utilization,
        CAST(Memory_Usage AS FLOAT) AS Memory_Usage,
        CAST(Disk_IO AS FLOAT) AS Disk_IO,
        CAST(Network_Traffic_In AS FLOAT) AS Network_Traffic_In,
        CAST(Network_Traffic_Out AS FLOAT) AS Network_Traffic_Out,
        CAST(Uptime_Hours AS FLOAT) AS Uptime_Hours,
        CAST(Downtime_Hours AS FLOAT) AS Downtime_Hours,
        CAST(Uptime_Hours AS FLOAT) + CAST(Downtime_Hours AS FLOAT) AS Total_Hours,
        CAST(Uptime_Hours AS FLOAT) * 100.0 / NULLIF(CAST(Uptime_Hours AS FLOAT) + CAST(Downtime_Hours AS FLOAT),0) AS Availability_Percentage,
        CAST(Network_Traffic_In AS FLOAT) + CAST(Network_Traffic_Out AS FLOAT) AS Total_Network_Traffic,
        CASE WHEN CAST(CPU_Utilization AS FLOAT) > 85 THEN 'Critical'
             WHEN CAST(CPU_Utilization AS FLOAT) > 70 THEN 'High'
             ELSE 'Normal' END AS CPU_Status,
        CASE WHEN CAST(Memory_Usage AS FLOAT) > 90 THEN 'Critical'
             WHEN CAST(Memory_Usage AS FLOAT) > 75 THEN 'High'
             ELSE 'Normal' END AS Memory_Status,
        CASE WHEN CAST(CPU_Utilization AS FLOAT) > 90 OR CAST(Memory_Usage AS FLOAT) > 90 THEN 'Warning'
             ELSE 'Healthy' END AS Server_Status,
        Admin_Name,
        Admin_Email,
        Admin_Phone,
        Log_Timestamp,
        DATEPART(YEAR, CONVERT(DATE, LEFT(Log_Timestamp, 10), 105)) AS Log_Year,
        DATEPART(MONTH, CONVERT(DATE, LEFT(Log_Timestamp, 10), 105)) AS Log_Month,
        DATEPART(DAY, CONVERT(DATE, LEFT(Log_Timestamp, 10), 105)) AS Log_Day
    INTO GOLD.ServerMetrics_Business
    FROM silver.ServerMetrics_Transform;
END;

exec etl.gold_server_monitoring_data