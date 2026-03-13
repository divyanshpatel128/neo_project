

create schema silver;

CREATE OR ALTER PROCEDURE etl.silver_server_monitoring_data
AS
BEGIN
    SET NOCOUNT ON;

    -- Drop the target table if it already exists
    IF OBJECT_ID('silver.ServerMetrics_Transform', 'U') IS NOT NULL
    BEGIN
        DROP TABLE silver.ServerMetrics_Transform;
    END;

    -- Create the new transformed table
    SELECT
        Server_ID,
        Hostname,
        CONCAT(LEFT(IP_Address, 7), '***.***') AS IP_Address,
        OS_Type,
        REPLACE(Server_Location_City, '"', '')     AS Server_Location_City,
        REPLACE(Server_Location_Country, '"', '')  AS Server_Location_Country,

        CAST(CPU_Utilization AS DECIMAL(5,2))      AS CPU_Utilization,
        CAST(Memory_Usage AS DECIMAL(5,2))         AS Memory_Usage,
        CAST(Disk_IO AS DECIMAL(5,2))              AS Disk_IO,

        CAST(Network_Traffic_In AS FLOAT)          AS Network_Traffic_In,
        CAST(Network_Traffic_Out AS FLOAT)         AS Network_Traffic_Out,

        CAST(Uptime_Hours AS FLOAT)                AS Uptime_Hours,
        CAST(Downtime_Hours AS FLOAT)              AS Downtime_Hours,

        CAST(Uptime_Hours AS FLOAT) + 
        CAST(Downtime_Hours AS FLOAT)              AS Total_Hours,

        Admin_Name,
        Admin_Email,
        CONCAT('XXXX-', RIGHT(Admin_Phone, 4)) AS Admin_Phone,
        Log_Timestamp  

    INTO
        silver.ServerMetrics_Transform
    FROM
        bronze.ServerMetrics;
END;

exec etl.silver_server_monitoring_data