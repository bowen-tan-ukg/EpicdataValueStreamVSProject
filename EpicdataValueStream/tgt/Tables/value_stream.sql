CREATE TABLE [tgt].[value_stream] (
    [pillar_key]             NVARCHAR (70)  NULL,
    [value_stream_key]       NVARCHAR (70)  NULL,
    [sub_value_stream_key]   NVARCHAR (70)  NULL,
    [value_stream_id]        NVARCHAR (60)  NULL,
    [sub_value_stream_id]    NVARCHAR (60)  NULL,
    [pillar_id]              NVARCHAR (60)  NULL,
    [value_stream_guuid]     NVARCHAR (60)  NULL,
    [sub_value_stream_guuid] NVARCHAR (60)  NOT NULL,
    [pillar_guuid]           NVARCHAR (60)  NULL,
    [value_stream_name]      NVARCHAR (300) NULL,
    [pillar_name]            NVARCHAR (200) NULL,
    [sub_value_stream_name]  NVARCHAR (200) NULL,
    [value_stream_type]      NVARCHAR (100) NULL,
    [value_stream_info_key]  NVARCHAR (70)  NULL,
    [start_date]             DATETIME2 (0)  NULL,
    PRIMARY KEY CLUSTERED ([sub_value_stream_guuid] ASC)
);

