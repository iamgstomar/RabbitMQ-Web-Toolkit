-- ============================================================
-- RMQ QA Utility  --  SQL Server Setup Script
-- Run this in SSMS to create the database and all tables.
-- ============================================================

-- 1. Create the database (skip if already exists)
--IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'RMQ_Events')
--BEGIN
--    CREATE DATABASE [RMQ_Events];
--    PRINT 'Database RMQ_Events created.';
--END
--ELSE
--    PRINT 'Database RMQ_Events already exists.';
--GO

--USE [RMQ_Events];
--GO

-- ============================================================
-- 2. audit_log  --  every action taken in or detected by the app
-- ============================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'audit_log'
)
BEGIN
    CREATE TABLE [dbo].[audit_log] (
        [id]         BIGINT        IDENTITY(1,1) PRIMARY KEY,
        [timestamp]  DATETIME2     NOT NULL DEFAULT GETDATE(),
        [user]       NVARCHAR(200) NULL,
        [host]       NVARCHAR(300) NULL,
        [action]     NVARCHAR(200) NULL,
        [detail]     NVARCHAR(MAX) NULL,
        [status]     NVARCHAR(100) NULL,
        [source]     NVARCHAR(200) NULL,
        [vhost]      NVARCHAR(300) NULL,
        [created_at] DATETIME2     NOT NULL DEFAULT GETDATE()
    );

    CREATE INDEX [IX_audit_log_timestamp] ON [dbo].[audit_log] ([timestamp] DESC);
    CREATE INDEX [IX_audit_log_user]      ON [dbo].[audit_log] ([user]);
    CREATE INDEX [IX_audit_log_action]    ON [dbo].[audit_log] ([action]);
    CREATE INDEX [IX_audit_log_source]    ON [dbo].[audit_log] ([source]);

    PRINT 'Table audit_log created.';
END
ELSE
    PRINT 'Table audit_log already exists.';
GO

-- ============================================================
-- 3. rmq_events  --  raw events from amq.rabbitmq.event exchange
-- ============================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'rmq_events'
)
BEGIN
    CREATE TABLE [dbo].[rmq_events] (
        [id]          BIGINT        IDENTITY(1,1) PRIMARY KEY,
        [timestamp]   DATETIME2     NOT NULL DEFAULT GETDATE(),
        [routing_key] NVARCHAR(300) NULL,
        [user]        NVARCHAR(200) NULL,
        [vhost]       NVARCHAR(300) NULL,
        [resource]    NVARCHAR(500) NULL,
        [action]      NVARCHAR(200) NULL,
        [source]      NVARCHAR(200) NULL,
        [raw_event]   NVARCHAR(MAX) NULL,       -- full JSON payload
        [dedup_key]   NVARCHAR(500) NULL,
        [created_at]  DATETIME2     NOT NULL DEFAULT GETDATE()
    );

    CREATE INDEX [IX_rmq_events_timestamp]   ON [dbo].[rmq_events] ([timestamp] DESC);
    CREATE INDEX [IX_rmq_events_routing_key] ON [dbo].[rmq_events] ([routing_key]);
    CREATE UNIQUE INDEX [IX_rmq_events_dedup] ON [dbo].[rmq_events] ([dedup_key])
        WHERE [dedup_key] IS NOT NULL;

    PRINT 'Table rmq_events created.';
END
ELSE
    PRINT 'Table rmq_events already exists.';
GO

-- ============================================================
-- 4. rmq_snapshots  --  periodic state snapshots (queues, users etc.)
-- ============================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'rmq_snapshots'
)
BEGIN
    CREATE TABLE [dbo].[rmq_snapshots] (
        [id]         BIGINT        IDENTITY(1,1) PRIMARY KEY,
        [timestamp]  DATETIME2     NOT NULL DEFAULT GETDATE(),
        [snap_type]  NVARCHAR(50)  NULL,        -- 'queues', 'users', 'exchanges' etc.
        [host]       NVARCHAR(300) NULL,
        [data]       NVARCHAR(MAX) NULL,         -- JSON snapshot
        [created_at] DATETIME2     NOT NULL DEFAULT GETDATE()
    );

    CREATE INDEX [IX_rmq_snapshots_timestamp] ON [dbo].[rmq_snapshots] ([timestamp] DESC);
    CREATE INDEX [IX_rmq_snapshots_type]      ON [dbo].[rmq_snapshots] ([snap_type]);
    CREATE INDEX [IX_rmq_snapshots_host]      ON [dbo].[rmq_snapshots] ([host]);

    PRINT 'Table rmq_snapshots created.';
END
ELSE
    PRINT 'Table rmq_snapshots already exists.';
GO

-- ============================================================
-- 5. Create a dedicated SQL login for the app (optional)
--    Change the password before running!
-- ============================================================
/*
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'rmq_qa_user')
BEGIN
    CREATE LOGIN [rmq_qa_user] WITH PASSWORD = 'StrongPassword123!';
    PRINT 'Login rmq_qa_user created.';
END

USE [RMQ_Events];

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'rmq_qa_user')
BEGIN
    CREATE USER [rmq_qa_user] FOR LOGIN [rmq_qa_user];
    GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO [rmq_qa_user];
    PRINT 'User rmq_qa_user created and granted access.';
END
*/

-- ============================================================
-- 6. Verify everything
-- ============================================================
SELECT
    TABLE_NAME,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c
     WHERE c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = 'dbo') AS column_count
FROM INFORMATION_SCHEMA.TABLES t
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME IN ('audit_log', 'rmq_events', 'rmq_snapshots')
ORDER BY TABLE_NAME;

PRINT '';
PRINT 'Setup complete. Update config.json with your SQL Server credentials.';
GO
