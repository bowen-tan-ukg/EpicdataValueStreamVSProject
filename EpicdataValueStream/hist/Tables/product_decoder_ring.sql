CREATE TABLE [hist].[product_decoder_ring] (
    [id]                    NVARCHAR (70)  NOT NULL,
    [ProductName]           NVARCHAR (120) NULL,
    [ServiceProductline]    NVARCHAR (50)  NULL,
    [Solution]              NVARCHAR (40)  NULL,
    [pillar_name]           NVARCHAR (40)  NULL,
    [value_stream_name]     NVARCHAR (140) NULL,
    [sub_value_stream_name] NVARCHAR (165) NULL,
    [created_by]            NVARCHAR (100) NULL,
    [created_at]            DATETIME2 (0)  NULL,
    [start_date]            DATETIME2 (0)  NULL,
    [end_date]              DATETIME2 (0)  NULL
);

