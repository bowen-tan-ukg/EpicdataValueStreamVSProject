
CREATE  PROC [tgt].[sp_merge_valuestream_land_to_tgt]
AS
BEGIN
    SET NOCOUNT ON; 
        DROP TABLE IF EXISTS  #valuestream
         
        DROP TABLE IF EXISTS #source_valuestream


    select * into #source_valuestream from [land].[value_stream]
    
    create table #valuestream
    (
        [pillar_key]				nvarchar(70),
		[value_stream_key]			nvarchar(70),
		[sub_value_stream_key]		nvarchar(70),
		[value_stream_id]			nvarchar(60),
		[sub_value_stream_id]		nvarchar(60),
		[pillar_id]					nvarchar(60),
		[value_stream_guuid]		nvarchar(60),
		[sub_value_stream_guuid]	nvarchar(60),
		[pillar_guuid]				nvarchar(60),
		[value_stream_name]			nvarchar(300),
		[pillar_name]				nvarchar(200),
		[sub_value_stream_name]		nvarchar(200),
		[value_stream_type]			nvarchar(100),
		[value_stream_info_key]		nvarchar(70),
        [start_date]                datetime,
        ActionType                  nvarchar(100)
    )

    MERGE   [tgt].[value_stream]      AS target 
        USING   #source_valuestream    AS Source 
        on Source.sub_value_stream_guuid = target.sub_value_stream_guuid
        -- For Not Matched
        WHEN NOT MATCHED BY target THEN
        INSERT 
        (
        [pillar_key],
		[value_stream_key],
		[sub_value_stream_key],
		[value_stream_id],
		[sub_value_stream_id],
		[pillar_id],
		[value_stream_guuid],
		[sub_value_stream_guuid],
		[pillar_guuid],
		[value_stream_name],
		[pillar_name],
		[sub_value_stream_name],
		[value_stream_type],
		[value_stream_info_key],
        [start_date]
        )
        Values
        (
        [pillar_key],
		[value_stream_key],
		[sub_value_stream_key],
		[value_stream_id],
		[sub_value_stream_id],
		[pillar_id],
		[value_stream_guuid],
		[sub_value_stream_guuid],
		[pillar_guuid],
		[value_stream_name],
		[pillar_name],
		[sub_value_stream_name],
		[value_stream_type],
		[value_stream_info_key],
        getdate()
        )
    -- For Matched
    WHEN MATCHED AND
         ISNULL(Target.[pillar_key], '')                   <>          ISNULL(Source.[pillar_key],'')
	  OR ISNULL(Target.[value_stream_key], '')             <>          ISNULL(Source.[value_stream_key],'')
      OR ISNULL(Target.[sub_value_stream_key], '')         <>          ISNULL(Source.[sub_value_stream_key],'')
      OR ISNULL(Target.[value_stream_id], '')              <>          ISNULL(Source.[value_stream_id],'')
	  OR ISNULL(Target.[sub_value_stream_id], '')          <>          ISNULL(Source.[sub_value_stream_id],'')
      OR ISNULL(Target.[pillar_id], '')                    <>          ISNULL(Source.[pillar_id],'')
      OR ISNULL(Target.[value_stream_guuid], '')           <>          ISNULL(Source.[value_stream_guuid],'')
      OR ISNULL(Target.[sub_value_stream_guuid], '')       <>          ISNULL(Source.[sub_value_stream_guuid],'')
      OR ISNULL(Target.[pillar_guuid], '')                 <>          ISNULL(Source.[pillar_guuid],'')
      OR ISNULL(Target.[value_stream_name], '')            <>          ISNULL(Source.[value_stream_name],'')
      OR ISNULL(Target.[pillar_name], '')                  <>          ISNULL(Source.[pillar_name],'')
      OR ISNULL(Target.[sub_value_stream_name], '')        <>          ISNULL(Source.[sub_value_stream_name],'')
      OR ISNULL(Target.[value_stream_type], '')            <>          ISNULL(Source.[value_stream_type],'')
      OR ISNULL(Target.[value_stream_info_key], '')        <>          ISNULL(Source.[value_stream_info_key],'')
    THEN UPDATE SET
        Target.[pillar_key]                    =  Source.[pillar_key]
		,Target.[value_stream_key]             =  Source.[value_stream_key]
		,Target.[sub_value_stream_key]         =  Source.[sub_value_stream_key]
		,Target.[value_stream_id]              =  Source.[value_stream_id]
		,Target.[sub_value_stream_id]          =  Source.[sub_value_stream_id]
		,Target.[pillar_id]                    =  Source.[pillar_id]
		,Target.[value_stream_guuid]           =  Source.[value_stream_guuid]
		,Target.[sub_value_stream_guuid]       =  Source.[sub_value_stream_guuid]
		,Target.[pillar_guuid]                 =  Source.[pillar_guuid]
		,Target.[value_stream_name]            =  Source.[value_stream_name]
		,Target.[pillar_name]                  =  Source.[pillar_name]
		,Target.[sub_value_stream_name]        =  Source.[sub_value_stream_name]
		,Target.[value_stream_type]            =  Source.[value_stream_type]
		,Target.[value_stream_info_key]        =  Source.[value_stream_info_key]
        ,Target.[start_date]                   =  getdate()
    OUTPUT  CASE WHEN $action = 'INSERT' THEN Inserted.[pillar_key]
                 ELSE Deleted.[pillar_key] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_key]
                 ELSE Deleted.[value_stream_key] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[sub_value_stream_key]
                 ELSE Deleted.[sub_value_stream_key] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_id]
                 ELSE Deleted.[value_stream_id] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[sub_value_stream_id]
                 ELSE Deleted.[sub_value_stream_id] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[pillar_id]
                 ELSE Deleted.[pillar_id] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_guuid]
                 ELSE Deleted.[value_stream_guuid] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[sub_value_stream_guuid]
                 ELSE Deleted.[sub_value_stream_guuid] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[pillar_guuid]
                 ELSE Deleted.[pillar_guuid] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_name]
                 ELSE Deleted.[value_stream_name] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[pillar_name]
                 ELSE Deleted.[pillar_name] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[sub_value_stream_name]
                 ELSE Deleted.[sub_value_stream_name] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_type]
                 ELSE Deleted.[value_stream_type] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_info_key]
                 ELSE Deleted.[value_stream_info_key] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[start_date]
                 ELSE Deleted.[start_date] END ,
        $action INTO #valuestream ([pillar_key],[value_stream_key],[sub_value_stream_key],[value_stream_id],[sub_value_stream_id],[pillar_id],[value_stream_guuid],
			[sub_value_stream_guuid],[pillar_guuid],[value_stream_name],[pillar_name],[sub_value_stream_name],[value_stream_type],[value_stream_info_key],[start_date],ActionType);

    Insert INTO hist.value_stream ([pillar_key],[value_stream_key],[sub_value_stream_key],[value_stream_id],[sub_value_stream_id],[pillar_id],[value_stream_guuid],
			[sub_value_stream_guuid],[pillar_guuid],[value_stream_name],[pillar_name],[sub_value_stream_name],[value_stream_type],[value_stream_info_key],[start_date],[end_date])
    select [pillar_key],[value_stream_key],[sub_value_stream_key],[value_stream_id],[sub_value_stream_id],[pillar_id],[value_stream_guuid],
			[sub_value_stream_guuid],[pillar_guuid],[value_stream_name],[pillar_name],[sub_value_stream_name],[value_stream_type],[value_stream_info_key],[start_date],getdate()
        from #valuestream where ActionType = 'UPDATE'

END