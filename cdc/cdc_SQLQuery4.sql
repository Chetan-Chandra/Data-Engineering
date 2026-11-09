USE AdventureWorksDW2022;
GO

INSERT INTO dbo.CDC_LearningCustomer
(
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
)
VALUES
(
    10,
    'Rahul Das',
    'Kolkata',
    'rahul@gmail.com',
    SYSUTCDATETIME()
);

INSERT INTO dbo.CDC_LearningCustomer
(
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
)
VALUES
(
    11,
    'Priya Sharma',
    'Delhi',
    'priya@gmail.com',
    SYSUTCDATETIME()
);

USE AdventureWorksDW2022;
GO

SELECT
    __$start_lsn,
    __$operation,
    CustomerID,
    CustomerName,
    City,
    Email
FROM cdc.dbo_CDC_LearningCustomer_CT
WHERE CustomerID IN (10, 11)
ORDER BY __$start_lsn;

USE AdventureWorksDW2022;
GO

SELECT *
FROM dbo.CDC_LearningCustomer
WHERE CustomerID IN (10, 11);

CustomerID	CustomerName	City	Email	UpdatedAt
10	Rahul Das	Kolkata	rahul@gmail.com	2026-11-07 10:26:29.3684783
11	Priya Sharma	Delhi	priya@gmail.com	2026-11-07 10:29:11.8613623


USE AdventureWorksDW2022;
GO

EXEC sys.sp_cdc_help_jobs;
job_id	job_type	job_name	maxtrans	maxscans	continuous	pollinginterval	retention	threshold
6E4BCD10-FC53-4B8B-9C89-F3D0FBC89437	capture	cdc.AdventureWorksDW2022_capture	500	10	1	5	0	0
51467770-B0D9-47B4-819C-D09B4BAA417D	cleanup	cdc.AdventureWorksDW2022_cleanup	0	0	0	0	4320	5000

SELECT TOP 10
    session_id,
    start_time,
    end_time,
    scan_phase,
    error_count,
    tran_count,
    last_commit_cdc_time
FROM sys.dm_cdc_log_scan_sessions
ORDER BY session_id DESC;
session_id	start_time	end_time	scan_phase	error_count	tran_count	last_commit_cdc_time

USE AdventureWorksDW2022;
GO

EXEC msdb.dbo.sp_help_job
    @job_name = 'cdc.AdventureWorksDW2022_capture';

job_id	originating_server	name	enabled	description	start_step_id	category	owner	notify_level_eventlog	notify_level_email	notify_level_netsend	notify_level_page	notify_email_operator	notify_netsend_operator	notify_page_operator	delete_level	date_created	date_modified	version_number	last_run_date	last_run_time	last_run_outcome	next_run_date	next_run_time	next_run_schedule_id	current_execution_status	current_execution_step	current_retry_attempt	has_step	has_schedule	has_target	type
6E4BCD10-FC53-4B8B-9C89-F3D0FBC89437	CHETAN\SQLSERVER2022	cdc.AdventureWorksDW2022_capture	1	CDC Log Scan Job	1	REPL-LogReader	sa	2	0	0	0	(unknown)	(unknown)	(unknown)	0	2026-11-02 21:09:11.737	2026-11-02 21:09:11.817	3	0	0	5	0	0	0	1	2 (Change Data Capture Collection Agent)	0	2	1	1	1


step_id	step_name	subsystem	command	flags	cmdexec_success_code	on_success_action	on_success_step_id	on_fail_action	on_fail_step_id	server	database_name	database_user_name	retry_attempts	retry_interval	os_run_priority	output_file_name	last_run_outcome	last_run_duration	last_run_retries	last_run_date	last_run_time	proxy_id
1	Starting Change Data Capture Collection Agent	TSQL	RAISERROR(22801, 10, -1)	4 ((include results in history))	0	3 (goto next step)	0	3 (goto next step)	0	Chetan\SQLSERVER2022	AdventureWorksDW2022	NULL	10	1	0 (normal)	NULL	1	0	0	20261107	160338	NULL
2	Change Data Capture Collection Agent	TSQL	sys.sp_MScdc_capture_job	4 ((include results in history))	0	1 (quit with success)	0	2 (quit with failure)	0	Chetan\SQLSERVER2022	AdventureWorksDW2022	NULL	10	1	0 (normal)	NULL	0	0	0	0	0	NULL


schedule_id	schedule_name	enabled	freq_type	freq_interval	freq_subday_type	freq_subday_interval	freq_relative_interval	freq_recurrence_factor	active_start_date	active_end_date	active_start_time	active_end_time	date_created	schedule_description	next_run_date	next_run_time	schedule_uid	job_count
11	CDC capture agent schedule.	1	64	0	0	0	0	0	20261102	99991231	0	235959	2026-11-02 21:09:11.843	(Description not requested.)	0	0	59BAC513-4A1C-42D1-9737-EF43D3853556	1

server_id	server_name	enlist_date	last_poll_date	last_run_date	last_run_time	last_run_duration	last_run_outcome	last_outcome_message
0	Chetan\SQLSERVER2022	1998-11-13 00:00:00.000	1998-11-13 00:00:00.000	0	0	0	5	NULL


USE AdventureWorksDW2022;
GO

SELECT
    __$start_lsn,
    __$operation,
    CustomerID,
    CustomerName,
    City,
    Email
FROM cdc.dbo_CDC_LearningCustomer_CT
WHERE CustomerID IN (10, 11)
ORDER BY __$start_lsn;

__$start_lsn	__$operation	CustomerID	CustomerName	City	Email
0x0000003400004FC80003	2	10	Rahul Das	Kolkata	rahul@gmail.com
0x0000003400004FD00003	3	10	Rahul Das	Kolkata	rahul@gmail.com
0x0000003400004FD00003	4	10	Rahul Das	Bangalore	rahul@gmail.com
0x0000003400004FD80005	1	10	Rahul Das	Bangalore	rahul@gmail.com
0x0000003400004FE00006	2	10	Rahul Das	Kolkata	rahul@gmail.com
0x0000003400004FE80003	2	11	Priya Sharma	Delhi	priya@gmail.com

USE AdventureWorksDW2022;
GO

INSERT INTO dbo.CDC_LearningCustomer
(
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
)
VALUES
(
    12,
    'Test Customer',
    'Kolkata',
    'test12@gmail.com',
    GETDATE()
);
GO