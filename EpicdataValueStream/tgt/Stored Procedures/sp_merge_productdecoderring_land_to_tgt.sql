
CREATE  PROC [tgt].[sp_merge_productdecoderring_land_to_tgt]
AS
BEGIN
    SET NOCOUNT ON; 
        DROP TABLE IF EXISTS  #productdecoderring
         
        DROP TABLE IF EXISTS #source_productdecoderring


    select * into #source_productdecoderring from [land].[product_decoder_ring]
    
    create table #productdecoderring
    (
        [id]				        nvarchar(70),
		[ProductName]			    nvarchar(120),
		[ServiceProductline]		nvarchar(50),
		[Solution]			        nvarchar(40),
		[pillar_name]		        nvarchar(40),
		[value_stream_name]			nvarchar(140),
		[sub_value_stream_name]		nvarchar(165),
		[created_by]	            nvarchar(100),
		[created_at]				datetime,
        [start_date]                datetime,
        ActionType                  nvarchar(100)
    )

		MERGE   [tgt].[product_decoder_ring]      AS target 
        USING   #source_productdecoderring    AS Source 
        on Source.id = target.id
        -- For Not Matched
        WHEN NOT MATCHED BY target THEN
        INSERT 
        (
		 [id],
		 [ProductName],
		 [ServiceProductline],
		 [Solution],
		 [pillar_name],
		 [value_stream_name],
		 [sub_value_stream_name],
		 [created_by],
		 [created_at],
         [start_date]
        )
        Values
        (
		 [id],
		 [ProductName],
		 [ServiceProductline],
		 [Solution],
		 [pillar_name],
		 [value_stream_name],
		 [sub_value_stream_name],
		 [created_by],
		 case when Source.created_at = '' then null else cast(Source.created_at as datetime) end,
         getdate()
        )
    -- For Matched
    WHEN MATCHED AND
         ISNULL(Target.[id], '')                      <>          ISNULL(Source.[id],'')
	  OR ISNULL(Target.[ProductName], '')             <>          ISNULL(Source.[ProductName],'')
      OR ISNULL(Target.[ServiceProductline], '')      <>          ISNULL(Source.[ServiceProductline],'')
      OR ISNULL(Target.[Solution], '')                <>          ISNULL(Source.[Solution],'')
	  OR ISNULL(Target.[pillar_name], '')             <>          ISNULL(Source.[pillar_name],'')
      OR ISNULL(Target.[value_stream_name], '')       <>          ISNULL(Source.[value_stream_name],'')
      OR ISNULL(Target.[sub_value_stream_name], '')   <>          ISNULL(Source.[sub_value_stream_name],'')
      OR ISNULL(Target.[created_by], '')              <>          ISNULL(Source.[created_by],'')
      OR ISNULL(Target.[created_at], '')              <>          ISNULL(Source.[created_at],'')
    THEN UPDATE SET
        Target.[id]                       =  Source.[id]
		,Target.[ProductName]             =  Source.[ProductName]
		,Target.[ServiceProductline]      =  Source.[ServiceProductline]
		,Target.[Solution]                =  Source.[Solution]
		,Target.[pillar_name]             =  Source.[pillar_name]
		,Target.[value_stream_name]       =  Source.[value_stream_name]
		,Target.[sub_value_stream_name]   =  Source.[sub_value_stream_name]
		,Target.[created_by]              =  Source.[created_by]
		,Target.[created_at]              =  Source.[created_at]
        ,Target.[start_date]              =  getdate()
    OUTPUT  CASE WHEN $action = 'INSERT' THEN Inserted.[id]
                 ELSE Deleted.[id] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[ProductName]
                 ELSE Deleted.[ProductName] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[ServiceProductline]
                 ELSE Deleted.[ServiceProductline] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[Solution]
                 ELSE Deleted.[Solution] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[pillar_name]
                 ELSE Deleted.[pillar_name] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[value_stream_name]
                 ELSE Deleted.[value_stream_name] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[sub_value_stream_name]
                 ELSE Deleted.[sub_value_stream_name] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[created_by]
                 ELSE Deleted.[created_by] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[created_at]
                 ELSE Deleted.[created_at] END ,
		    CASE WHEN $action = 'INSERT' THEN Inserted.[start_date]
                 ELSE Deleted.[start_date] END ,
        $action INTO #productdecoderring ([id],[ProductName],[ServiceProductline],[Solution],[pillar_name],[value_stream_name],
			[sub_value_stream_name],[created_by],[created_at],[start_date],ActionType);

    Insert INTO hist.product_decoder_ring([id],[ProductName],[ServiceProductline],[Solution],[pillar_name],[value_stream_name],
			[sub_value_stream_name],[created_by],[created_at],[start_date],[end_date])
    select [id],[ProductName],[ServiceProductline],[Solution],[pillar_name],[value_stream_name],
			[sub_value_stream_name],[created_by],[created_at],[start_date],getdate()
        from #productdecoderring where ActionType = 'UPDATE'

END