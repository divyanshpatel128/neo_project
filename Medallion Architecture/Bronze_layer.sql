
create schema bronze;

CREATE TABLE bronze.ServerMetrics (
    Server_ID              VARCHAR(100)      NOT NULL,
    Hostname               VARCHAR(100)     NOT NULL,
    IP_Address             VARCHAR(100)      NOT NULL,
    OS_Type                VARCHAR(100)      NOT NULL,
    Server_Location_City        VARCHAR(100)     NOT NULL,
    Server_Location_Country        VARCHAR(100)     NOT NULL,
    CPU_Utilization        VARCHAR(100)           NOT NULL,
    Memory_Usage           VARCHAR(100)            NOT NULL,
    Disk_IO                VARCHAR(100)           NOT NULL,
    Network_Traffic_In     VARCHAR(100)            NOT NULL,
    Network_Traffic_Out    VARCHAR(100)           NOT NULL,
    Uptime_Hours           VARCHAR(100)            NOT NULL,
    Downtime_Hours         VARCHAR(100)            NOT NULL,
    Admin_Name             VARCHAR(100)     NOT NULL,
    Admin_Email            VARCHAR(100)     NOT NULL,
    Admin_Phone            VARCHAR(100)      NOT NULL,
    Log_Timestamp          VARCHAR(1000)      NOT NULL,
);



--- bronze layer 


create or alter procedure etl.bronze_server_monitoring_data
as
begin 
	IF OBJECT_ID('bronze.ServerMetrics', 'U') IS NOT NULL
    BEGIN
        TRUNCATE TABLE bronze.ServerMetrics;
    END;

    BULK INSERT bronze.ServerMetrics
    FROM 'E:\assigment\neostats\Virtual Server Monitoring and Performance Data engineer use case\Virtual Server Monitoring and Performance\Sample_Data_Ingestion.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
        );

end

exec etl.bronze_server_monitoring_data
