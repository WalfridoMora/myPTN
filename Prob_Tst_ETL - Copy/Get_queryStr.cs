namespace Prob_Tst_ETL
{
    class Get_queryStr
    {
        
        public string myQuery(int queryNumber)
        {
            switch (queryNumber)
            {
                case 1:
                    return "DELETE FROM AGGREG.PTNTB0011_DIST_CNT_INF";
                    
                case 2:
                    return "DELETE FROM AGGREG.PTNTB0012_QCP_PROD_INFO";
                    
                case 3:
                    return "DELETE FROM AGGREG.PTNTB0013_QCP_MAC_FCLTY_INFO";
                    
                case 4:
                    return "DELETE FROM AGGREG.PTNTB0014_PRODUCTS";
                    
                case 5:
                    return "DELETE FROM AGGREG.PTNTB0015_MINE_TO_TERMINALS";
                    
                                    
                case 7:
                    return @"DECLARE @MTRL_TYP_CD VARCHAR(6)
                             SET @MTRL_TYP_CD = 'AGGREG';

                                               SELECT DISTINCT P1.PROD_SQ,
            	                               P1.FCLTY_ID,
            	                               PROD_CD,
            	                               PROC1,
            	                               PROD_NM,
            	                               PROD_STAT_CD,
            	                               P1.UPDT_TMS,
            	                               PROD_NOTE

                                        FROM
            	                              (SELECT PINFO.PROD_SQ,
            			                              PINFO.FCLTY_ID,
            	                                      PINFO.PROD_CD,
            		                                  PINFO.PROC1,
            		                                  PINFO.PROD_NM,
            		                                  PSTAT.PROD_STAT_CD,
            		                                  PSTAT.UPDT_TMS,
            			                              PSTAT.PROD_STAT_RSN_TXT AS PROD_NOTE
            	                               FROM
            			                              (SELECT	T226.PROD_SQ,
            		                                            T079.FCLTY_ID,
            		                                            T225.PROD_CD, 
            		                                            IIf([PRCS_NUM] Is Null,1,[PRCS_NUM]) AS PROC1,
            		                                            T225.PROD_NM 

            				                            FROM
            						                            (dbo.MACT225_PROD_DFIN AS T225
            						                            INNER JOIN
            						                            dbo.MACT226_PRODUCT AS T226
            						                            ON 
            						                            (T225.PROD_DFIN_SQ = T226.PROD_DFIN_SQ)
            						                            AND
            						                            (T225.MTRL_SQ = T226.MTRL_SQ)) 
            						                            INNER JOIN
            						                            (dbo.MACT070_FCLTY AS T070
            						                            INNER JOIN
            						                            dbo.MACT079_PRODFCLTY AS T079
            						                            ON
            						                            T070.FCLTY_SQ = T079.FCLTY_SQ)
            						                            ON T226.FCLTY_SQ = T070.FCLTY_SQ
            				                            WHERE MTRL_TYP_CD = @MTRL_TYP_CD) AS PINFO

            				                            JOIN

            				                            dbo.MACT227_PRODSTAT AS PSTAT

            				                            ON PINFO.PROD_SQ = PSTAT.PROD_SQ ) AS P1

            				                            JOIN

            				                            (SELECT PSTAT.PROD_SQ,
            						                            MAX(PSTAT.UPDT_TMS) AS UPDT_TMS
            						                            FROM
            						                                 dbo.MACT227_PRODSTAT AS PSTAT
            							                             GROUP BY PSTAT.PROD_SQ) AS P2
            				                            ON 
            				                            P1.PROD_SQ = P2.PROD_SQ
            				                            AND
            				                            P1.UPDT_TMS = P2.UPDT_TMS

            				                            LEFT JOIN
            							                            (SELECT  MINE_TO_TERMINALS.ORIG_PROD_SQ,
            									                             MINE_TO_TERMINALS.RCV_PROD_SQ,
            									                             T079_1.FCLTY_ID
            							                            FROM
            							                            [MACDWSQL1].[dbo].[MACT224_PROD_TRNFR] AS MINE_TO_TERMINALS

            							                            JOIN

            							                            dbo.MACT079_PRODFCLTY AS T079_1
            							                            ON T079_1.FCLTY_SQ = MINE_TO_TERMINALS.RCV_FCLTY_SQ) AS MINES_TO_TER

            							                            ON MINES_TO_TER.ORIG_PROD_SQ = P2.PROD_SQ  ";


                case 8:
                    return @"
                        DECLARE @MTRL_TYP_CD VARCHAR(6)
                        SET @MTRL_TYP_CD = 'AGGREG';
                        SELECT T226.PROD_SQ, 
                        IIF([FDOT_MNG_DIST_CD] IS NULL, [FDOT_GEOG_DIST_CD], [FDOT_MNG_DIST_CD]) 
                        AS DIST, 
                        T079.FCLTY_ID, 
                        T070.FCLTY_DS, 
                        T225.PROD_CD, 
                        IIF([PRCS_NUM] IS NULL, 1, [PRCS_NUM]) 
                        AS PROC1, 
                        T225.PROD_NM, 
                        T226.PROD_FCLTY_CLSF_CD, 
                        T226.MTRL_TYP_CD 
                        FROM   (DBO.MACT225_PROD_DFIN AS T225 
                                INNER JOIN DBO.MACT226_PRODUCT AS T226 
                                        ON ( T225.PROD_DFIN_SQ = T226.PROD_DFIN_SQ ) 
                                            AND ( T225.MTRL_SQ = T226.MTRL_SQ )) 
                                INNER JOIN (DBO.MACT070_FCLTY AS T070 
                                            INNER JOIN DBO.MACT079_PRODFCLTY AS T079 
                                                    ON T070.FCLTY_SQ = T079.FCLTY_SQ) 
                                        ON T226.FCLTY_SQ = T070.FCLTY_SQ  
                                        WHERE MTRL_TYP_CD = @MTRL_TYP_CD";

                case 9:
                    return @"
                    SELECT 0 AS ID,
                            ORIG_PROD_SQ, 
                            RCV_PROD_SQ 
                    FROM   [MACDWSQL1].[DBO].[MACT224_PROD_TRNFR] 
                    WHERE  ORIG_PROD_SQ IS NOT NULL 
                            AND RCV_PROD_SQ IS NOT NULL  
            ";

                case 10:
                    return @"
            			DECLARE @MY_DATE DATETIME
            SET @MY_DATE = GETDATE(); --The Date reference for Last 30 Samples at most 1 year old is TODAY
            DECLARE @MTRL_TYP_CD VARCHAR(6)
			SET @MTRL_TYP_CD = 'AGGREG';--Material type = aggregate
			DECLARE @STAT_CD VARCHAR(4)
			SET @STAT_CD = 'CMPL';--Sample is completed
			DECLARE @SMPL_LVL_CD CHAR(1)
			SET @SMPL_LVL_CD = 'Q';--It is a QC Sample
			DECLARE @SMPL_TYP_CD VARCHAR(8)
			SET @SMPL_TYP_CD = 'ATSOURCE';--Sample was taken at source
			DECLARE @EXCL_RUN_THIRTY_CD CHAR(1)
			SET @EXCL_RUN_THIRTY_CD = 'N';--Not excluded from Running Thirty
			DECLARE @TST_CMPLT_CD CHAR(1)
			SET @TST_CMPLT_CD = 'Y'; --TEST IS COMPLETED ON SMPL_NUM
			DECLARE @PROD_STAT_CD_EXPIRED VARCHAR(6)
			SET @PROD_STAT_CD_EXPIRED = 'EXPIRED';

            SET nocount ON 

            IF OBJECT_ID('tempdb.dbo.##PTNTB0070_30_SMPLS', 'U') IS NOT NULL -- If Temp Table ##PTNTB0070_30_SMPLS exists --> we drop it
                           DROP TABLE ##PTNTB0070_30_SMPLS;            				
					
		    SELECT  LAST_30_.[PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
					,TST_DFIN_ID
	                INTO ##PTNTB0070_30_SMPLS --We store temporarily All samples taken for each product, finalized and tested for each TST_DFIN_ID
            FROM
            (
            SELECT   [PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
					,TST_DFIN_ID
	        FROM
	                (
            SELECT   [PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,SMPL_X_PROD_1.[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
					,TST_DFIN_ID
            FROM
                (
                SELECT  [PROD_SQ]
						,[MTRL_TYP_CD]
						,T100.[SMPL_NUM]
						,[SMPL_DT]
						,[STAT_CD]
						,TST_DFIN_ID
                FROM [dbo].[MACT100_SMPL] AS T100

                JOIN

                DBO.MACT102_TST_RSLT AS T102
                ON 
                T102.SMPL_NUM = T100.SMPL_NUM
                WHERE       [STAT_CD] = @STAT_CD --Sample is completed
		                AND [MTRL_TYP_CD] = @MTRL_TYP_CD --Material type = aggregate
		                AND [SMPL_DT] >= @MY_DATE -365 --SAMPLES AT MOST 1 YEAR OLD 
		                AND SMPL_LVL_CD = @SMPL_LVL_CD --It is a QC Sample
		                AND SMPL_TYP_CD = SMPL_TYP_CD --Sample was taken at source
		                AND ( (EXCL_RUN_THIRTY_CD IS NULL) OR (EXCL_RUN_THIRTY_CD = @EXCL_RUN_THIRTY_CD)) --SAMPLE IS NOT EXCLUDED FROM RUNNING THIRTY
		                AND (T102.TST_CMPLT_CD =@TST_CMPLT_CD) 	--TEST IS COMPLETED ON SMPL_NUM					
                ) AS SMPL_X_PROD_1

                JOIN
				--We need to guarantee that Samples have results on table T103
                (
                SELECT DISTINCT SMPL_NUM
                FROM
                dbo.MACT103_TSTRSLTSTP AS T103
                
                ) AS SMPL_X_PROD_2
                ON SMPL_X_PROD_1.SMPL_NUM = SMPL_X_PROD_2.SMPL_NUM
            

            EXCEPT --*****WE ARE EXCLUDING SAMPLES TAKEN BEFORE THE LAST CLOCK RESET********

            SELECT   SMPL_X_PROD.[PROD_SQ]
			        ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
	                ,SMPL_X_PROD.TST_DFIN_ID	
	
                FROM
                (
                SELECT  [PROD_SQ]
						,[MTRL_TYP_CD]
						,SMPL_X_PROD_1.[SMPL_NUM]
						,[SMPL_DT]
						,[STAT_CD]
						,TST_DFIN_ID
                FROM
                (
                SELECT   [PROD_SQ]
						,[MTRL_TYP_CD]
						,T100.[SMPL_NUM]
						,[SMPL_DT]
						,[STAT_CD]
                FROM [dbo].[MACT100_SMPL] AS T100
                INNER JOIN
                DBO.MACT102_TST_RSLT AS T102
                ON 
                T102.SMPL_NUM = T100.SMPL_NUM
                WHERE [STAT_CD] = @STAT_CD 
				AND [MTRL_TYP_CD] = @MTRL_TYP_CD 
				AND [SMPL_DT] >= @MY_DATE -365  
				AND SMPL_LVL_CD = @SMPL_LVL_CD 
				AND SMPL_TYP_CD = @SMPL_TYP_CD 
				AND ( (EXCL_RUN_THIRTY_CD IS NULL) OR (EXCL_RUN_THIRTY_CD = @EXCL_RUN_THIRTY_CD)) 
                AND (T102.TST_CMPLT_CD =@TST_CMPLT_CD) --TEST IS COMPLETED ON SMPL_NUM
                ) AS SMPL_X_PROD_1

                INNER JOIN

                (
                SELECT DISTINCT SMPL_NUM, TST_DFIN_ID
                FROM
                dbo.MACT103_TSTRSLTSTP AS T103
                ) AS SMPL_X_PROD_2
                ON SMPL_X_PROD_1.SMPL_NUM = SMPL_X_PROD_2.SMPL_NUM
                ) AS SMPL_X_PROD

                JOIN
                (
                SELECT [PROD_SQ]
                    ,[TST_DFIN_ID]
                    ,MAX([RESET_CLOCK_DT]) AS [RESET_CLOCK_DT] --Latest Reset Clock Date
                FROM [MACDWSQL1].[dbo].[MACT222_PT_RSTCLK]
                GROUP BY [PROD_SQ]
                        ,[TST_DFIN_ID]
                        ,[RESET_CLOCK_DT]
                ) AS RC
                ON RC.[PROD_SQ] = SMPL_X_PROD.PROD_SQ
                AND RC.[TST_DFIN_ID] = SMPL_X_PROD.TST_DFIN_ID
                WHERE
                RC.RESET_CLOCK_DT > SMPL_X_PROD.SMPL_DT
                ) AS LAST_30
                ) AS LAST_30_
                

				JOIN

				(
				SELECT ST1.PROD_SQ, ST2.PROD_STAT_CD
				FROM
				(SELECT  PROD_SQ,  
						MAX(UPDT_TMS) AS DT 
				FROM
				dbo.MACT227_PRODSTAT
				GROUP BY PROD_SQ) AS ST1
				JOIN
				(SELECT PROD_SQ,  UPDT_TMS AS DT, PROD_STAT_CD 
				FROM
				dbo.MACT227_PRODSTAT) AS ST2
				ON
				ST1.DT = ST2.DT
				AND
				ST1.PROD_SQ = ST2.PROD_SQ
				WHERE [PROD_STAT_CD] <> @PROD_STAT_CD_EXPIRED
				) AS PSTAT
				ON
				PSTAT.PROD_SQ = LAST_30_.PROD_SQ

				CREATE NONCLUSTERED INDEX IDX_NC_ ON ##PTNTB0070_30_SMPLS(TST_DFIN_ID, PROD_SQ, SMPL_NUM)          						
			   		
                ";

                case 11:
                    return @"
                        			DECLARE @MTRL_TYP_CD VARCHAR(6)
			SET @MTRL_TYP_CD = 'AGGREG';--Material type = aggregate
			DECLARE @TST_CMPLT_CD CHAR(1)
			SET @TST_CMPLT_CD = 'Y'; --TEST IS COMPLETED ON SMPL_NUM
			DECLARE @TST_DFIN_ID VARCHAR(7)
			SET @TST_DFIN_ID = 'FM1T011';
			DECLARE @STEP_DS VARCHAR(26)
			SET @STEP_DS = 'Total Percent of Minus 200';

SET nocount ON

            IF OBJECT_ID('tempdb.dbo.#TL_000', 'U') IS NOT NULL
              DROP TABLE #TL_000;


                                SELECT REVERSE_KEYS.TST_DFIN_ID, PROD_SQ, REVERSE_KEYS.TST_DFIN_VER_NUM, REVERSE_KEYS.TST_SECT_SQ, REVERSE_KEYS.TST_DFIN_STEP_NUM, REVERSE_KEYS.STEP_DS, REVERSE_KEYS.SECT_TTL_TXT,
                                LOWER_LIMIT, TARG, UPPER_LIMIT
            					INTO #TL_000
                                FROM
                                (
                                SELECT[TST_DFIN_ID],[PROD_SQ], MAX([TST_DFIN_VER_NUM]) AS TST_DFIN_VER_NUM, [TST_SECT_SQ],[TST_DFIN_STEP_NUM], STEP_DS, SECT_TTL_TXT, LOWER_LIMIT
                                , TARG, UPPER_LIMIT
                                FROM
                                (
                          SELECT P_SQ AS PROD_SQ,
            	   TL_1.TST_DFIN_ID,
            	   TL_1.TST_DFIN_VER_NUM,
            	   TL_1.TST_SECT_SQ,
            	   TL_1.[TST_DFIN_STEP_NUM],
            	   STEP_DS,
            	   SECT_TTL_TXT
            	   ,'-' AS LOWER_LIMIT
            	   ,'-' AS TARG
            	   ,UPPER_LIMIT
            FROM
            (

            SELECT PROD_SQ AS P_SQ,
            	   TST_DFIN_ID,
            	   TST_DFIN_VER_NUM,
            	   TST_SECT_SQ,
            	   [TST_DFIN_STEP_NUM],
            	   LTRIM(RTRIM(TRGT_VAL)) AS TRGT_VAL
            	   , CAST(UPPER_LIMIT AS FLOAT) AS UPPER_LIMIT

            FROM
            (
            SELECT PROD_SQ,
            	   LIMITS.TST_DFIN_ID,
            	   LIMITS.TST_DFIN_VER_NUM,
            	   LIMITS.TST_SECT_SQ,
            	   LIMITS.[TST_DFIN_STEP_NUM],
            	   SMPL_TRGT_LMT_SQ
            	   ,
            	   TRGT_VAL, 
            	   UPPER_LIMIT FROM
            (
            SELECT PROD_SQ,
            	   LIMITS.TST_DFIN_ID,
            	   LIMITS.TST_DFIN_VER_NUM,
            	   LIMITS.TST_SECT_SQ,
            	   LIMITS.[TST_DFIN_STEP_NUM],
            	   SMPL_TRGT_LMT_SQ
            	   ,
            	   TRGT_VAL, 
            	   UPPER_LIMIT

            FROM
            (SELECT PROD_SQ,
            	   LIMITS.TST_DFIN_ID,
            	   LIMITS.TST_DFIN_VER_NUM,
            	   LIMITS.TST_SECT_SQ,
            	   LIMITS.[TST_DFIN_STEP_NUM],
            	   SMPL_TRGT_LMT_SQ
            	   ,
            	   TRGT_VAL, 
            	   UPPER_LIMIT
            	   , ROW_NUMBER() OVER (PARTITION BY PROD_SQ ORDER BY SMPL_TRGT_LMT_SQ  DESC) AS RowNum 
            	   FROM

            (
            --***************************************************SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NOT NULL********************(BEGING A)    
            SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.[TST_DFIN_ID], A.TST_DFIN_VER_NUM, A.TST_SECT_SQ
                  ,A.[TST_DFIN_STEP_NUM], SMPL_TRGT_LMT_SQ, B.SPEC_SQ, UPPER_LIMIT FROM
            (
            SELECT [TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
            	  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
            	  ,SMPL_TRGT_LMT_SQ
                  ,[SPEC_SQ]
                  ,[SPEC_CAT_SQ]
                  ,[PROD_SQ]
                  ,[TRGT_VAL]
                  ,[COND_VAL]
            	  , UPPER_LIMIT
              FROM
              ( SELECT  T1.*
                   ,TRY_CONVERT(FLOAT,IIF(CHARINDEX('=',S2)>0,LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('=',S2)))),LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('<',S2)))))) AS UPPER_LIMIT

            FROM
            	(SELECT TL.*,RIGHT(LTRIM(RTRIM(TRGT_VAL)),10) AS S2, LEN(RIGHT(LTRIM(RTRIM(TRGT_VAL)),10)) AS L2

            	  FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] AS TL
            	  WHERE TST_DFIN_ID = @TST_DFIN_ID) AS T1 

              )
              AS TRGTS


              WHERE TST_DFIN_VER_NUM = (

              SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
              FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE TST_DFIN_ID = @TST_DFIN_ID)
              AND TST_DFIN_ID = @TST_DFIN_ID
              AND SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NOT NULL) AS A

              --*************************************************************************************************************(END A)
              RIGHT JOIN 

              --************************************************************************************************************(BEGIN B)
            ( SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM
            (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ 
            FROM
            MACT006_SPECCAT AS T006
            WHERE PROD_DFIN_SQ IS NOT NULL
            GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
            ) AS SC

            JOIN


            (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

            FROM
            (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
            FROM
            MACT226_PRODUCT AS T226
            WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
            JOIN

            (SELECT T225.PROD_DFIN_SQ
            FROM
            MACT225_PROD_DFIN AS T225) AS P2
            ON
            P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
            ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ) AS B
             --************************************************************************************************************(END B)
             ON A.SPEC_SQ = B.SPEC_SQ
             WHERE TRGT_VAL IS NOT NULL

             --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

             UNION ALL

             --***************************************************SPEC_CAT_SQ IS NOT NULL AND SPEC_SQ IS NOT NULL********************(BEGING A)    
            SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.[TST_DFIN_ID], A.TST_DFIN_VER_NUM, A.TST_SECT_SQ
                  ,A.[TST_DFIN_STEP_NUM], SMPL_TRGT_LMT_SQ, B.SPEC_SQ, UPPER_LIMIT FROM
            (SELECT [TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
            	  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
            	  ,SMPL_TRGT_LMT_SQ
                  ,[SPEC_SQ]
                  ,[SPEC_CAT_SQ]
                  ,[PROD_SQ]
                  ,[TRGT_VAL]
                  ,[COND_VAL]
            	  ,UPPER_LIMIT
              FROM 
              ( SELECT  T1.*
                   ,TRY_CONVERT(FLOAT,IIF(CHARINDEX('=',S2)>0,LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('=',S2)))),LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('<',S2)))))) AS UPPER_LIMIT

            FROM
            	(SELECT TL.*,RIGHT(LTRIM(RTRIM(TRGT_VAL)),10) AS S2, LEN(RIGHT(LTRIM(RTRIM(TRGT_VAL)),10)) AS L2

            	  FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] AS TL
            	  WHERE TST_DFIN_ID = @TST_DFIN_ID) AS T1 

              ) 
              AS TRGTS


              WHERE TST_DFIN_VER_NUM = (

              SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
              FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE TST_DFIN_ID = @TST_DFIN_ID)
              AND TST_DFIN_ID = @TST_DFIN_ID
              AND SPEC_CAT_SQ IS NOT NULL AND SPEC_SQ IS NOT NULL) AS A

              --*************************************************************************************************************(END A)
              RIGHT JOIN 

              --************************************************************************************************************(BEGIN B)
            ( SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM
            (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ 
            FROM
            MACT006_SPECCAT AS T006
            WHERE PROD_DFIN_SQ IS NOT NULL
            GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
            ) AS SC

            JOIN


            (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

            FROM
            (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
            FROM
            MACT226_PRODUCT AS T226
            WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
            JOIN

            (SELECT T225.PROD_DFIN_SQ
            FROM
            MACT225_PROD_DFIN AS T225) AS P2
            ON
            P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
            ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ) AS B
             --************************************************************************************************************(END B)
             ON A.SPEC_SQ = B.SPEC_SQ AND A.SPEC_CAT_SQ = B.SPEC_CAT_SQ
             WHERE TRGT_VAL IS NOT NULL

             UNION ALL

             --***************************************************SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NULL********************(BEGING A) 
            SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.[TST_DFIN_ID], A.TST_DFIN_VER_NUM, A.TST_SECT_SQ
                  ,A.[TST_DFIN_STEP_NUM], SMPL_TRGT_LMT_SQ, B.SPEC_SQ, UPPER_LIMIT FROM
            (SELECT [TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
            	  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
            	  ,SMPL_TRGT_LMT_SQ
                  ,[SPEC_SQ]
                  ,[SPEC_CAT_SQ]
                  ,[PROD_SQ]
                  ,[TRGT_VAL]
                  ,[COND_VAL]
            	  , UPPER_LIMIT
              FROM
              ( SELECT  T1.*
                   ,TRY_CONVERT(FLOAT,IIF(CHARINDEX('=',S2)>0,LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('=',S2)))),LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('<',S2)))))) AS UPPER_LIMIT

            FROM
            	(SELECT TL.*,RIGHT(LTRIM(RTRIM(TRGT_VAL)),10) AS S2, LEN(RIGHT(LTRIM(RTRIM(TRGT_VAL)),10)) AS L2

            	  FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] AS TL
            	  WHERE TST_DFIN_ID = @TST_DFIN_ID) AS T1 

              )
              AS TRGTS


              WHERE TST_DFIN_VER_NUM = (

              SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
              FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE TST_DFIN_ID = @TST_DFIN_ID)
              AND TST_DFIN_ID = @TST_DFIN_ID
              AND SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NULL) AS A

              --*************************************************************************************************************(END A)
              RIGHT JOIN 

              --************************************************************************************************************(BEGIN B)
            (  SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM
            (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ 
            FROM
            MACT006_SPECCAT AS T006
            WHERE PROD_DFIN_SQ IS NOT NULL
            GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
            ) AS SC

            LEFT JOIN



            (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

            FROM
            (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
            FROM
            MACT226_PRODUCT AS T226
            WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
            JOIN

            (SELECT T225.PROD_DFIN_SQ
            FROM
            MACT225_PROD_DFIN AS T225) AS P2
            ON
            P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
            ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ ) AS B
             --************************************************************************************************************(END B)
             ON A.PROD_SQ = B.PROD_SQ
             WHERE TRGT_VAL IS NOT NULL

             ) AS LIMITS
             ) as LIMITS
             WHERE RowNum = 1
             ) as LIMITS

             LEFT JOIN

             (
             SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            FROM
            dbo.MACT021_TST_STEP AS T021
            INNER JOIN
            dbo.MACT015_TST_SECT AS T015
            ON
            (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
            AND
            (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
            AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
            GROUP BY
            T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            HAVING

            (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
            AND((T021.STEP_DS) = @STEP_DS)
            ) 
            )AS KEY_VALUES
            ON
            KEY_VALUES.TST_DFIN_ID = LIMITS.TST_DFIN_ID
            AND 
            KEY_VALUES.TST_DFIN_VER_NUM= LIMITS.TST_DFIN_VER_NUM
            AND 
            KEY_VALUES.TST_SECT_SQ= LIMITS.TST_SECT_SQ
            AND 
            KEY_VALUES.TST_DFIN_STEP_NUM= LIMITS.TST_DFIN_STEP_NUM
            WHERE
            PROD_SQ NOT IN(SELECT DISTINCT [PROD_SQ]    
              FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE PROD_SQ IS NOT NULL and TST_DFIN_ID = @TST_DFIN_ID)

              ) AS TL_SPEC                                                    

              UNION

              SELECT T.[PROD_SQ]
                  ,T.[TST_DFIN_ID]
                  ,T.[TST_DFIN_VER_NUM]
                  ,T.[TST_SECT_SQ]
                  ,T.[TST_DFIN_STEP_NUM]
                  ,T.[TRGT_VAL]
            	  , CAST(UPPER_LIMIT AS FLOAT) AS UPPER_LIMIT
                  FROM 
            	  ( SELECT  T1.*
                   ,TRY_CONVERT(FLOAT,IIF(CHARINDEX('=',S2)>0,LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('=',S2)))),LTRIM(RTRIM(RIGHT(LTRIM(RTRIM(S2)),L2 -CHARINDEX('<',S2)))))) AS UPPER_LIMIT

            FROM
            	(SELECT TL.*,RIGHT(LTRIM(RTRIM(TRGT_VAL)),10) AS S2, LEN(RIGHT(LTRIM(RTRIM(TRGT_VAL)),10)) AS L2

            	  FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] AS TL
            	  WHERE TST_DFIN_ID = @TST_DFIN_ID AND PROD_SQ IS NOT NULL) AS T1 

              ) AS T --ORDER BY PROD_SQ
              JOIN

              (
              SELECT *
            FROM
            (SELECT TL.PROD_SQ, TL.TST_DFIN_VER_NUM, TST_SECT_SQ, TST_DFIN_STEP_NUM,  MAX(SMPL_TRGT_LMT_SQ) AS SMPL_TRGT_LMT_SQ--, TL.TRGT_VAL
            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] AS TL
            	  WHERE TST_DFIN_ID = @TST_DFIN_ID AND PROD_SQ IS NOT NULL 
            	  GROUP BY  PROD_SQ, TST_DFIN_VER_NUM, TST_SECT_SQ, TST_DFIN_STEP_NUM) AS Q1

            	  JOIN

            	   (SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM_ FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] WHERE TST_DFIN_ID = 'FM1T011') AS Q2
            	   ON Q1.TST_DFIN_VER_NUM = Q2.TST_DFIN_VER_NUM_
              ) AS TT
              ON T.PROD_SQ = TT.PROD_SQ
                 AND
            	 T.TST_DFIN_VER_NUM= TT.TST_DFIN_VER_NUM
                 AND
            	 T.TST_SECT_SQ= TT.TST_SECT_SQ
            	 AND
            	 T.TST_DFIN_STEP_NUM = TT.TST_DFIN_STEP_NUM
            	 AND 
            	 T.SMPL_TRGT_LMT_SQ = TT.SMPL_TRGT_LMT_SQ -- ORDER BY PROD_SQ
            	  --WHERE PROD_SQ IS NOT NULL
            	  --      AND MTRL_TYP_CD = 'AGGREG'
            			--AND [TST_DFIN_ID] = 'FM1T011'
            			--AND [TST_DFIN_VER_NUM] = (SELECT MAX([TST_DFIN_VER_NUM]) AS [TST_DFIN_VER_NUM]
            			--								FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
            			--								WHERE [TST_DFIN_ID] = 'FM1T011')


              ) AS TL_1

              LEFT JOIN

             (
             SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            FROM
            dbo.MACT021_TST_STEP AS T021
            INNER JOIN
            dbo.MACT015_TST_SECT AS T015
            ON
            (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
            AND
            (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
            AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
            GROUP BY
            T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            HAVING

            (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
            AND((T021.STEP_DS) = @STEP_DS)
            ) 
            )AS KEY_VALUES
            ON
            KEY_VALUES.TST_DFIN_ID = TL_1.TST_DFIN_ID
            AND 
            KEY_VALUES.TST_DFIN_VER_NUM= TL_1.TST_DFIN_VER_NUM
            AND 
            KEY_VALUES.TST_SECT_SQ= TL_1.TST_SECT_SQ
            AND 
            KEY_VALUES.TST_DFIN_STEP_NUM= TL_1.TST_DFIN_STEP_NUM
                        ) AS ToTL_3
                        GROUP BY
                        [TST_DFIN_ID],[PROD_SQ],[TST_DFIN_VER_NUM], [TST_SECT_SQ],[TST_DFIN_STEP_NUM], STEP_DS, SECT_TTL_TXT, LOWER_LIMIT
                        , TARG, UPPER_LIMIT --, CMP_SYM
                        ) AS TL_
                        JOIN
                        (
                        SELECT T021.TST_DFIN_ID, T021.TST_DFIN_VER_NUM, T021.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM, T021.STEP_DS, T015.SECT_TTL_TXT
                        FROM
                        [dbo].[MACT021_TST_STEP] AS T021
                        INNER JOIN
                        [dbo].[MACT015_TST_SECT] AS T015
                        ON
                        T021.TST_DFIN_ID = T015.TST_DFIN_ID
                        AND
                        T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM
                        AND
                        T021.TST_SECT_SQ = T015.TST_SECT_SQ
                        ) AS REVERSE_KEYS
                        ON
                        REVERSE_KEYS.TST_DFIN_ID = TL_.TST_DFIN_ID
                        AND
                        REVERSE_KEYS.STEP_DS = TL_.STEP_DS
                        AND
                        REVERSE_KEYS.SECT_TTL_TXT = TL_.SECT_TTL_TXT

                        CREATE CLUSTERED INDEX IDX_C_PROD_SQ ON #TL_000(PROD_SQ)

                                    			
            SELECT R05.PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, COUNT_SMPL, MINIMUM, AVERAGE, MAXIMUM, ST_DEV, CAST(LOWER_LIMIT AS VARCHAR) AS LOWER_LIMIT, CAST(TARG AS VARCHAR) AS TARG, UPPER_LIMIT, Z
                            FROM
                            (
                            SELECT *
                            FROM
                        (
                        SELECT R03.*, ROUND(TRY_CONVERT(FLOAT, IIF(Z_L<Z_U, Z_L, Z_U)),3) AS Z
                        FROM
                        (
                        SELECT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, CAST([MINIMUM] AS FLOAT) AS MINIMUM, AVERAGE, CAST([MAXIMUM] AS FLOAT) AS MAXIMUM, ST_DEV,
            			IIF(ST_DEV <> 0, IIF(LOWER_LIMIT<>0,ABS((LOWER_LIMIT - AVERAGE) / ST_DEV),1000), 1000) AS Z_L, IIF(ST_DEV <> 0, IIF(UPPER_LIMIT<>100,ABS((UPPER_LIMIT - AVERAGE) / ST_DEV),1000), 1000) AS Z_U, COUNT_SMPL
                        FROM


                        (

                        SELECT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, MIN(VAL_NUM) AS MINIMUM, ROUND(TRY_CONVERT(FLOAT, AVG(VAL_NUM)), 6) AS AVERAGE, MAX(VAL_NUM)AS MAXIMUM,
                                ROUND(TRY_CONVERT(FLOAT, STDEV(VAL_NUM)), 6) AS ST_DEV, COUNT(SMPL_NUM) AS COUNT_SMPL --CMP_SYM, 
                                FROM
                                (

                                SELECT PROD_SQ, T1.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, T1.SMPL_NUM, VAL_NUM--CMP_SYM, 
                                FROM
                                (
                                SELECT DISTINCT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, SMPL_NUM, VAL_NUM --CMP_SYM, 
            					 from
                                (
                                SELECT TL_001.PROD_SQ, TL_001.TST_DFIN_ID, TL_001.STEP_DS, TL_001.SECT_TTL_TXT, TL_001.LOWER_LIMIT, TL_001.TARG, TL_001.UPPER_LIMIT, RESULTS.SMPL_NUM, RESULTS.VAL_NUM --TL_001.CMP_SYM, 
                                FROM
                                (
                                SELECT #TL_000.PROD_SQ, #TL_000.TST_DFIN_ID, #TL_000.TST_DFIN_VER_NUM, #TL_000.TST_SECT_SQ, #TL_000.TST_DFIN_STEP_NUM, #TL_000.STEP_DS, #TL_000.SECT_TTL_TXT, #TL_000.LOWER_LIMIT,
                               #TL_000.TARG, #TL_000.UPPER_LIMIT, ##PTNTB0071_30_SMPLS_FM1T011.SMPL_NUM--TL_000.CMP_SYM, 
            			FROM
                        #TL_000
            			JOIN
            			##PTNTB0071_30_SMPLS_FM1T011
            			ON
                        ##PTNTB0071_30_SMPLS_FM1T011.[PROD_SQ] = #TL_000.PROD_SQ
                        ) AS TL_001
                        JOIN
                        (
                        SELECT*
                        FROM
                        dbo.MACT103_TSTRSLTSTP AS T103
                        WHERE T103.TST_DFIN_ID = @TST_DFIN_ID
                        ) AS RESULTS
                        ON
                        RESULTS.TST_DFIN_ID = TL_001.TST_DFIN_ID
                        AND
                        RESULTS.TST_DFIN_VER_NUM = TL_001.TST_DFIN_VER_NUM
                        AND
                        RESULTS.TST_SECT_SQ = TL_001.TST_SECT_SQ
                        AND
                        RESULTS.TST_DFIN_STEP_NUM = TL_001.TST_DFIN_STEP_NUM
                        AND
                        RESULTS.SMPL_NUM = TL_001.SMPL_NUM
                        ) AS TL_0020

                        ) AS T1
                        INNER JOIN
                        (
                        SELECT SMPL_NUM, TST_DFIN_ID
                        FROM

                        dbo.MACT102_TST_RSLT AS T102
                        WHERE TST_CMPLT_CD = @TST_CMPLT_CD
                        ) AS T2
                        ON
                        T2.SMPL_NUM = T1.SMPL_NUM
                        AND
                        T2.TST_DFIN_ID = T1.TST_DFIN_ID

                        ) AS TL_004
                        GROUP BY
                        PROD_SQ,TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT,TARG,UPPER_LIMIT--,CMP_SYM
                        --HAVING LOWER_LIMIT<> TARG AND UPPER_LIMIT<> TARG


                        ) AS R02
                        WHERE
                        ST_DEV IS NOT NULL
                        --AND TARG > 10 
                        --AND TARG< 90 
                        ) AS R03
                        ) AS R04
                        WHERE COUNT_SMPL > 2

                        ) AS R05

            			DROP TABLE #TL_000
            			DROP TABLE ##PTNTB0071_30_SMPLS_FM1T011 

            ";

                case 12:
                    return @"--CREATE TEMP TABLE WITH ALL * ***FM1T011 * ***SAMPLES * *********************************BEGIN

            DECLARE @TST_DFIN_ID VARCHAR(7)
            SET @TST_DFIN_ID = 'FM1T011';

IF OBJECT_ID('tempdb.dbo.##PTNTB0071_30_SMPLS_FM1T011', 'U') IS NOT NULL
               DROP TABLE ##PTNTB0071_30_SMPLS_FM1T011;
			SELECT[PROD_SQ]
                    ,[SMPL_NUM]
                    , TST_DFIN_ID

                    INTO ##PTNTB0071_30_SMPLS_FM1T011
            FROM
            (
            SELECT[PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
                    , TST_DFIN_ID
                    , ROW_NUMBER() OVER(PARTITION BY[PROD_SQ] ORDER BY[PROD_SQ], [SMPL_DT] DESC) AS RowNum  FROM ##PTNTB0070_30_SMPLS --Assigns a row number for samples grouped by Product oredered by Date, so we can take the las 30 later on
					WHERE TST_DFIN_ID = @TST_DFIN_ID
            ) AS MySamples

            WHERE RowNum < 31


            CREATE NONCLUSTERED INDEX IDX_NC_PROD_SQ__SMPL_NUM ON ##PTNTB0071_30_SMPLS_FM1T011(SMPL_NUM, PROD_SQ)
			--CREATE TEMP TABLE WITH ALL * ***FM1T011 * ***SAMPLES * *********************************END";
                    
                case 13:
                    return @"
                        DECLARE @TST_DFIN_ID VARCHAR(10)
                        SET @TST_DFIN_ID = 'T27AggGrad';
                        DECLARE @MTRL_TYP_CD VARCHAR(6)
                        SET @MTRL_TYP_CD = 'AGGREG';
                        DECLARE @PROD_DATA_TYP_CD CHAR(1)
                        SET @PROD_DATA_TYP_CD = 'P';
                        DECLARE @STEP_DS_1 VARCHAR(21)
                        SET @STEP_DS_1 = 'Total Percent Passing';
                        DECLARE @STEP_DS_2 VARCHAR(16)
                        SET @STEP_DS_2 = 'Fineness Modulus';
                        DECLARE @TST_CMPLT_CD CHAR(1)
                        SET @TST_CMPLT_CD = 'Y';

 



            SET nocount ON

            IF OBJECT_ID('tempdb.dbo.#TL_000', 'U') IS NOT NULL
              DROP TABLE #TL_000;

              SELECT REVERSE_KEYS.TST_DFIN_ID, PROD_SQ, REVERSE_KEYS.TST_DFIN_VER_NUM, REVERSE_KEYS.TST_SECT_SQ, REVERSE_KEYS.TST_DFIN_STEP_NUM, REVERSE_KEYS.STEP_DS, REVERSE_KEYS.SECT_TTL_TXT,
                                LOWER_LIMIT, TARG, UPPER_LIMIT--, CMP_SYM
                                INTO #TL_000
                                FROM
                                (
                                SELECT[TST_DFIN_ID],[PROD_SQ], MAX([TST_DFIN_VER_NUM]) AS TST_DFIN_VER_NUM, [TST_SECT_SQ],[TST_DFIN_STEP_NUM], STEP_DS, SECT_TTL_TXT, LOWER_LIMIT
                                , TARG, UPPER_LIMIT--, CMP_SYM
                                FROM
                                (--***********************************************************************************+++++++++++++++++++++++++++++++++++++
                          SELECT P_SQ AS PROD_SQ,
                   TL_2.TST_DFIN_ID,
                   TL_2.TST_DFIN_VER_NUM,
                   TL_2.TST_SECT_SQ,
                   TL_2.[TST_DFIN_STEP_NUM],
                   TRGT_VAL,
                   KEY_VALUES.STEP_DS, KEY_VALUES.SECT_TTL_TXT,
                   (
                   IIF(TARG1 = '-', CAST(LL AS FLOAT), (CAST(TARG1 AS FLOAT) - CAST(MINUS AS FLOAT)))
                   ) AS LOWER_LIMIT,
                   (
                   IIF(TARG1 = '-', CAST(LL AS FLOAT), CAST(TARG1 AS FLOAT))

                   ) AS TARG,
                   (
                   IIF(TARG1 = '-', ABS(CAST(UL1 AS FLOAT) - CAST(UL AS FLOAT)), CAST(TARG1 AS FLOAT) + CAST(PLUS AS FLOAT))

                   ) AS UPPER_LIMIT

            FROM
            (
            SELECT P_SQ,
                   TST_DFIN_ID,
                   TST_DFIN_VER_NUM,
                   TST_SECT_SQ,

                   [TST_DFIN_STEP_NUM],
                   TRGT_VAL,
                   --, STEP_DS, SECT_TTL_TXT,
                   (
                   --If TRGT_VAL starts with a number-- > this number is the Lower_limit-- > the las part of the string is a number representing the Upper_Limit

                   IIF(ISNUMERIC(SUBSTRING(TRGT_VAL, 1, 1)) = 1, (IIF(ISNUMERIC(SUBSTRING(TRGT_VAL, 1, 2)) = 1, SUBSTRING(TRGT_VAL, 1, 2), SUBSTRING(TRGT_VAL, 1, 1))),
                   --If TRGT_VAL doesn't start with a number --> there are three possibilities:
                   -- = Target +/ -Offset-- > Upper_Limit and Lower_Limit  e.g.Total Percent Passing = 19 +/ -10
                   -- = Target + Upper_Offset / -Lower_Offset e.g.Total Percent Passing = 8 + 92 / -8
                   -- <= or >= Upper_Limit-- > There is not Lower_limit


                    '0')
                   ) as LL,
                   IIF(ISNUMERIC(SUBSTRING(TRGT_VAL, 1, 1)) = 1, REVERSE(SUBSTRING(REVERSE(TRGT_VAL), 1, CHARINDEX('=', REVERSE(TRGT_VAL)) - 2)), --(len(TRGT_VAL) - CHARINDEX('=', REVERSE(TRGT_VAL))) - 1


                    '0'
                   ) AS UL,
                   IIF(ISNUMERIC(SUBSTRING(TRGT_VAL, 1, 1)) = 0 AND CHARINDEX('-', TRGT_VAL) = 0, REVERSE(SUBSTRING(REVERSE(TRGT_VAL), 1, CHARINDEX('=', REVERSE(TRGT_VAL)) - 2)), --(len(TRGT_VAL) - CHARINDEX('=', REVERSE(TRGT_VAL))) - 1


                    '0'
                   ) AS UL1,
                   IIF(CHARINDEX('Fineness', TRGT_VAL) > 0,
                   CONVERT(VARCHAR, (SELECT ROUND(CAST([VAL_AMT] AS FLOAT), 4, 0)

                        FROM[MACDWSQL1].[dbo].[MACT229_PRDMXDDATA] AS T229

                        WHERE[PROD_DATA_TYP_CD] = @PROD_DATA_TYP_CD and T229.PROD_SQ = P_SQ)) ,
            	   IIF(CHARINDEX('-', TRGT_VAL) > 0, SUBSTRING(TRGT_VAL, CHARINDEX('=', TRGT_VAL) + 2, CHARINDEX('+', TRGT_VAL) - 1 - CHARINDEX('=', TRGT_VAL) - 1), --(len(TRGT_VAL) - CHARINDEX('=', REVERSE(TRGT_VAL))) - 1


                    '-')
            	   ) AS TARG1,

                   IIF(CHARINDEX('-', TRGT_VAL) > 0, LTRIM(RTRIM(SUBSTRING(TRGT_VAL, CHARINDEX('-', TRGT_VAL) + 1, len(TRGT_VAL) - CHARINDEX('-', TRGT_VAL)))), '-' )          
                   AS MINUS,

                   (
                   IIF(CHARINDEX('+/-', TRGT_VAL) > 0, LTRIM(RTRIM(SUBSTRING(TRGT_VAL, CHARINDEX('-', TRGT_VAL) + 1, len(TRGT_VAL) - CHARINDEX('-', TRGT_VAL)))),
                   IIF(CHARINDEX('-', TRGT_VAL) > 0, LTRIM(RTRIM(SUBSTRING(TRGT_VAL, CHARINDEX('+', TRGT_VAL) + 1, (CHARINDEX('/', TRGT_VAL) - CHARINDEX('+', TRGT_VAL) - 1)))), '-'))
                   ) AS PLUS

                   FROM
                   (
            SELECT PROD_SQ AS P_SQ,
                   TST_DFIN_ID,
                   TST_DFIN_VER_NUM,
                   TST_SECT_SQ,

                   [TST_DFIN_STEP_NUM],
                   LTRIM(RTRIM(TRGT_VAL)) AS TRGT_VAL
                   --, STEP_DS, SECT_TTL_TXT

            FROM
            (
            SELECT PROD_SQ,
                   LIMITS.TST_DFIN_ID,
                   LIMITS.TST_DFIN_VER_NUM,
                   LIMITS.TST_SECT_SQ,
                   LIMITS.[TST_DFIN_STEP_NUM],
                   TRGT_VAL--, KEY_VALUES.STEP_DS, KEY_VALUES.SECT_TTL_TXT

                   FROM

            (
            --***************************************************SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NOT NULL * *******************(BEGING A)
            SELECT DISTINCT B.PROD_SQ, A.TRGT_VAL, A.[TST_DFIN_ID], A.TST_DFIN_VER_NUM, A.TST_SECT_SQ
                  , A.[TST_DFIN_STEP_NUM], B.SPEC_SQ FROM
            (
            SELECT[TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
                  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
                  ,[SPEC_SQ]
                  ,[SPEC_CAT_SQ]
                  ,[PROD_SQ]
                  ,[TRGT_VAL]
                  ,[COND_VAL]
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              AS TRGTS


              WHERE TST_DFIN_VER_NUM = (

              SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE TST_DFIN_ID = @TST_DFIN_ID)
              AND TST_DFIN_ID = @TST_DFIN_ID
              AND SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NOT NULL) AS A

              -- * ************************************************************************************************************(END A)
              RIGHT JOIN

              -- * ***********************************************************************************************************(BEGIN B)
            (SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM

           (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ

           FROM

           MACT006_SPECCAT AS T006

           WHERE PROD_DFIN_SQ IS NOT NULL

           GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
            ) AS SC

            JOIN


            (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

            FROM
            (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
            FROM
            MACT226_PRODUCT AS T226
            WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
            JOIN

            (SELECT T225.PROD_DFIN_SQ
            FROM
            MACT225_PROD_DFIN AS T225) AS P2
            ON
            P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
            ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ) AS B
             --************************************************************************************************************(END B)
             ON A.SPEC_SQ = B.SPEC_SQ
             WHERE TRGT_VAL IS NOT NULL

             --++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ +

             UNION ALL

             -- * **************************************************SPEC_CAT_SQ IS NOT NULL AND SPEC_SQ IS NOT NULL********************(BEGING A)    
            SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.[TST_DFIN_ID], A.TST_DFIN_VER_NUM, A.TST_SECT_SQ
                  ,A.[TST_DFIN_STEP_NUM], B.SPEC_SQ FROM
            (SELECT[TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
                  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
                  ,[SPEC_SQ]
                  ,[SPEC_CAT_SQ]
                  ,[PROD_SQ]
                  ,[TRGT_VAL]
                  ,[COND_VAL]
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              AS TRGTS


              WHERE TST_DFIN_VER_NUM = (

              SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE TST_DFIN_ID = @TST_DFIN_ID)
              AND TST_DFIN_ID = @TST_DFIN_ID
              AND SPEC_CAT_SQ IS NOT NULL AND SPEC_SQ IS NOT NULL) AS A

              --*************************************************************************************************************(END A)
              RIGHT JOIN

              --************************************************************************************************************(BEGIN B)
            (SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM

           (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ

           FROM

           MACT006_SPECCAT AS T006

           WHERE PROD_DFIN_SQ IS NOT NULL

           GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
            ) AS SC

            JOIN


            (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

            FROM
            (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
            FROM
            MACT226_PRODUCT AS T226
            WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
            JOIN

            (SELECT T225.PROD_DFIN_SQ
            FROM
            MACT225_PROD_DFIN AS T225) AS P2
            ON
            P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
            ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ) AS B
             --************************************************************************************************************(END B)
             ON A.SPEC_SQ = B.SPEC_SQ AND A.SPEC_CAT_SQ = B.SPEC_CAT_SQ
             WHERE TRGT_VAL IS NOT NULL

             UNION ALL

             -- * **************************************************SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NULL********************(BEGING A) 
            SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.[TST_DFIN_ID], A.TST_DFIN_VER_NUM, A.TST_SECT_SQ
                  ,A.[TST_DFIN_STEP_NUM], B.SPEC_SQ FROM
            (SELECT[TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
                  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
                  ,[SPEC_SQ]
                  ,[SPEC_CAT_SQ]
                  ,[PROD_SQ]
                  ,[TRGT_VAL]
                  ,[COND_VAL]
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              AS TRGTS


              WHERE TST_DFIN_VER_NUM = (

              SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE TST_DFIN_ID = @TST_DFIN_ID)
              AND TST_DFIN_ID = @TST_DFIN_ID
              AND SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NULL) AS A

              --*************************************************************************************************************(END A)
              RIGHT JOIN

              --************************************************************************************************************(BEGIN B)
            (SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM

          (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ

          FROM

          MACT006_SPECCAT AS T006

          WHERE PROD_DFIN_SQ IS NOT NULL

          GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
            ) AS SC

            LEFT JOIN



            (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

            FROM
            (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
            FROM
            MACT226_PRODUCT AS T226
            WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
            JOIN

            (SELECT T225.PROD_DFIN_SQ
            FROM
            MACT225_PROD_DFIN AS T225) AS P2
            ON
            P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
            ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ ) AS B
             --************************************************************************************************************(END B)
             ON A.PROD_SQ = B.PROD_SQ
             WHERE TRGT_VAL IS NOT NULL

             ) AS LIMITS

             LEFT JOIN

             (
             SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            FROM
            dbo.MACT021_TST_STEP AS T021
            INNER JOIN
            dbo.MACT015_TST_SECT AS T015
            ON
            (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
            AND
            (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
            AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
            GROUP BY
            T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            HAVING

            (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
            AND((T021.STEP_DS) = @STEP_DS_1 or(T021.STEP_DS) = @STEP_DS_2)
            )
            )AS KEY_VALUES
            ON
            KEY_VALUES.TST_DFIN_ID = LIMITS.TST_DFIN_ID
            AND
            KEY_VALUES.TST_DFIN_VER_NUM = LIMITS.TST_DFIN_VER_NUM
            AND
            KEY_VALUES.TST_SECT_SQ = LIMITS.TST_SECT_SQ
            AND
            KEY_VALUES.TST_DFIN_STEP_NUM = LIMITS.TST_DFIN_STEP_NUM
            WHERE
            PROD_SQ NOT IN(SELECT DISTINCT[PROD_SQ]
              FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
              WHERE PROD_SQ IS NOT NULL)

              ) AS TL_SPEC

              UNION

              SELECT[PROD_SQ]
                  ,[TST_DFIN_ID]
                  ,[TST_DFIN_VER_NUM]
                  ,[TST_SECT_SQ]
                  ,[TST_DFIN_STEP_NUM]
                  ,[TRGT_VAL]
                  FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]

                  WHERE PROD_SQ IS NOT NULL
                        AND MTRL_TYP_CD = @MTRL_TYP_CD

                        AND[TST_DFIN_ID] = @TST_DFIN_ID

                        AND[TST_DFIN_VER_NUM] = (SELECT MAX([TST_DFIN_VER_NUM]) AS[TST_DFIN_VER_NUM]

                                                        FROM[MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]

                                                        WHERE[TST_DFIN_ID] = @TST_DFIN_ID)

              ) AS TL_1
              ) AS TL_2


             LEFT JOIN

             (
             SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            FROM
            dbo.MACT021_TST_STEP AS T021
            INNER JOIN
            dbo.MACT015_TST_SECT AS T015
            ON
            (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
            AND
            (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
            AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
            GROUP BY
            T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
            T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
            HAVING

            (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
            AND((T021.STEP_DS) = @STEP_DS_1 or(T021.STEP_DS) = @STEP_DS_2)
            )
            )AS KEY_VALUES
            ON
            KEY_VALUES.TST_DFIN_ID = TL_2.TST_DFIN_ID
            AND
            KEY_VALUES.TST_DFIN_VER_NUM = TL_2.TST_DFIN_VER_NUM
            AND
            KEY_VALUES.TST_SECT_SQ = TL_2.TST_SECT_SQ
            AND
            KEY_VALUES.TST_DFIN_STEP_NUM = TL_2.TST_DFIN_STEP_NUM
                        ) AS ToTL_3
                        GROUP BY
                        [TST_DFIN_ID],[PROD_SQ],[TST_DFIN_VER_NUM], [TST_SECT_SQ],[TST_DFIN_STEP_NUM], STEP_DS, SECT_TTL_TXT, LOWER_LIMIT
                        , TARG, UPPER_LIMIT --, CMP_SYM
                        ) AS TL_
                        JOIN
                        (
                        SELECT T021.TST_DFIN_ID, T021.TST_DFIN_VER_NUM, T021.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM, T021.STEP_DS, T015.SECT_TTL_TXT
                        FROM
                        [dbo].[MACT021_TST_STEP] AS T021
                        INNER JOIN
                        [dbo].[MACT015_TST_SECT] AS T015
                        ON
                        T021.TST_DFIN_ID = T015.TST_DFIN_ID
                        AND
                        T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM
                        AND
                        T021.TST_SECT_SQ = T015.TST_SECT_SQ
                        ) AS REVERSE_KEYS
                        ON
                        REVERSE_KEYS.TST_DFIN_ID = TL_.TST_DFIN_ID
                        AND
                        REVERSE_KEYS.STEP_DS = TL_.STEP_DS
                        AND
                        REVERSE_KEYS.SECT_TTL_TXT = TL_.SECT_TTL_TXT


                        CREATE CLUSTERED INDEX IDX_C_PROD_SQ ON #TL_000(PROD_SQ)

              --

              --

            SELECT R05.PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, COUNT_SMPL, MINIMUM, AVERAGE, MAXIMUM, ST_DEV,
            IIF(LOWER_LIMIT = 0 AND TARG = 0, '-', CAST(LOWER_LIMIT AS VARCHAR))AS LOWER_LIMIT,
            IIF(LOWER_LIMIT = 0 AND TARG = 0, '-', CAST(TARG AS VARCHAR)) AS TARG, UPPER_LIMIT,
                 IIF(CHARINDEX('F', STEP_DS) > 0,
                 ---1-- > ALERT - 2-- > OK
 
                 IIF((MINIMUM < LOWER_LIMIT) OR(MAXIMUM > UPPER_LIMIT), -1, -2)
                  , Z) AS Z
                            FROM
                            (
                            SELECT *
                            FROM
                        (
                        SELECT R03.*, ROUND(CONVERT(FLOAT, IIF(Z_L < Z_U, Z_L, Z_U)), 3) AS Z
                        FROM
                        (
                        SELECT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, CAST([MINIMUM] AS FLOAT) AS MINIMUM, AVERAGE, CAST([MAXIMUM] AS FLOAT) AS MAXIMUM, ST_DEV,
                        IIF(ST_DEV <> 0, IIF(LOWER_LIMIT <> 0, ABS((LOWER_LIMIT - AVERAGE) / ST_DEV), 1000), 1000) AS Z_L, IIF(ST_DEV <> 0, IIF(UPPER_LIMIT <> 100, ABS((UPPER_LIMIT - AVERAGE) / ST_DEV), 1000), 1000) AS Z_U, COUNT_SMPL
                        FROM


                        (

                        SELECT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, MIN(VAL_NUM) AS MINIMUM, ROUND(CONVERT(FLOAT, AVG(VAL_NUM)), 6) AS AVERAGE, MAX(VAL_NUM)AS MAXIMUM,
                                ROUND(CONVERT(FLOAT, STDEV(VAL_NUM)), 6) AS ST_DEV, COUNT(SMPL_NUM) AS COUNT_SMPL--CMP_SYM,
                                FROM
                                (

                                SELECT PROD_SQ, T1.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, T1.SMPL_NUM, VAL_NUM--CMP_SYM,
                                FROM
                                (
                                SELECT DISTINCT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT, SMPL_NUM, VAL_NUM--CMP_SYM,
                                 from
                                (
                                SELECT TL_001.PROD_SQ, TL_001.TST_DFIN_ID, TL_001.STEP_DS, TL_001.SECT_TTL_TXT, TL_001.LOWER_LIMIT, TL_001.TARG, TL_001.UPPER_LIMIT, RESULTS.SMPL_NUM, RESULTS.VAL_NUM--TL_001.CMP_SYM,
                                FROM
                                (
                                SELECT #TL_000.PROD_SQ, #TL_000.TST_DFIN_ID, #TL_000.TST_DFIN_VER_NUM, #TL_000.TST_SECT_SQ, #TL_000.TST_DFIN_STEP_NUM, #TL_000.STEP_DS, #TL_000.SECT_TTL_TXT, #TL_000.LOWER_LIMIT,
                               #TL_000.TARG, #TL_000.UPPER_LIMIT, ##PTNTB0072_30_SMPLS_T27AggGrad.SMPL_NUM--TL_000.CMP_SYM, 
            			FROM
                        #TL_000
            			JOIN
            			##PTNTB0072_30_SMPLS_T27AggGrad
            			ON
						##PTNTB0072_30_SMPLS_T27AggGrad.[PROD_SQ] = #TL_000.PROD_SQ

                        ) AS TL_001
                        JOIN
                        (
                        SELECT *
                        FROM
                        dbo.MACT103_TSTRSLTSTP AS T103
                        WHERE T103.TST_DFIN_ID = @TST_DFIN_ID
                        ) AS RESULTS
                        ON
                        RESULTS.TST_DFIN_ID = TL_001.TST_DFIN_ID
                        AND
                        RESULTS.TST_DFIN_VER_NUM = TL_001.TST_DFIN_VER_NUM
                        AND
                        RESULTS.TST_SECT_SQ = TL_001.TST_SECT_SQ
                        AND
                        RESULTS.TST_DFIN_STEP_NUM = TL_001.TST_DFIN_STEP_NUM
                        AND
                        RESULTS.SMPL_NUM = TL_001.SMPL_NUM
                        ) AS TL_0020

                        ) AS T1
                        INNER JOIN
                        (
                        SELECT SMPL_NUM, TST_DFIN_ID
                        FROM

                        dbo.MACT102_TST_RSLT AS T102
                        WHERE TST_CMPLT_CD = @TST_CMPLT_CD
                        ) AS T2
                        ON
                        T2.SMPL_NUM = T1.SMPL_NUM
                        AND
                        T2.TST_DFIN_ID = T1.TST_DFIN_ID

                        ) AS TL_004
                        GROUP BY
                        PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT--, CMP_SYM
                        --HAVING LOWER_LIMIT <> TARG AND UPPER_LIMIT <> TARG


                        ) AS R02
                        WHERE
                        ST_DEV IS NOT NULL
                        --AND TARG > 10
                        --AND TARG < 90
                        ) AS R03
                        ) AS R04
                        WHERE COUNT_SMPL > 2

                        ) AS R05


                        DROP TABLE #TL_000
            			DROP TABLE ##PTNTB0072_30_SMPLS_T27AggGrad 

            ";


                case 14:
                    return @"--CREATE TEMP TABLE WITH ALL ****T27AggGrad**** SAMPLES **********************************BEGIN

			DECLARE @TST_DFIN_ID VARCHAR(10)
SET @TST_DFIN_ID = 'T27AggGrad';

IF OBJECT_ID('tempdb.dbo.##PTNTB0072_30_SMPLS_T27AggGrad', 'U') IS NOT NULL
               DROP TABLE ##PTNTB0072_30_SMPLS_T27AggGrad;
			SELECT   [PROD_SQ]
                    ,[SMPL_NUM]
					,TST_DFIN_ID
					INTO ##PTNTB0072_30_SMPLS_T27AggGrad
            FROM
			(
			SELECT   [PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
					,TST_DFIN_ID
				    ,ROW_NUMBER() OVER (PARTITION BY [PROD_SQ] ORDER BY [PROD_SQ], [SMPL_DT] DESC) AS RowNum  FROM ##PTNTB0070_30_SMPLS--Assigns a row number for samples grouped by Product oredered by Date, so we can take the las 30 later on
					WHERE TST_DFIN_ID = @TST_DFIN_ID
			) AS MySamples
			WHERE RowNum <31
			
			CREATE NONCLUSTERED INDEX IDX_NC_PROD_SQ__SMPL_NUM ON ##PTNTB0072_30_SMPLS_T27AggGrad(SMPL_NUM, PROD_SQ )
			--CREATE TEMP TABLE WITH ALL ****T27AggGrad**** SAMPLES **********************************END";
                    
                case 15:
                    return @"
                       DECLARE @TST_DFIN_ID VARCHAR(11)
                        SET @TST_DFIN_ID = 'FM1T084FASG';
                        DECLARE @STEP_DS VARCHAR(21)
                        SET @STEP_DS = 'Bulk Specific Gravity';
                        DECLARE @SECT_TTL_TXT VARCHAR(4)
                        SET @SECT_TTL_TXT = 'Main';
                        DECLARE @PROD_DATA_TYP_CD CHAR(1)
                        SET @PROD_DATA_TYP_CD = 'N';
                        DECLARE @PROD_CD_C4 VARCHAR(4)
                        SET @PROD_CD_C4 = '%C4%';
                        DECLARE @PROD_CD_C5 VARCHAR(4)
                        SET @PROD_CD_C5 = '%C5%';
                        DECLARE @PROD_CD_F2 VARCHAR(4)
                        SET @PROD_CD_F2 = '%F2%';

                        SELECT R4.PROD_SQ,TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, COUNT_SMPL, MINIMUM, AVERAGE, MAXIMUM, ROUND(CONVERT(FLOAT, ST_DEV),6) AS ST_DEV, CONVERT(VARCHAR(6), (CONVERT(FLOAT, LOWER_LIMIT))) AS LOWER_LIMIT, CONVERT(VARCHAR(6), (CONVERT(FLOAT, TARG))) AS TARG, UPPER_LIMIT,
                   		                                    1.000 AS Z
                        FROM
                        (
                        SELECT COUNT(PROD_SQ) AS COUNT_SMPL, PROD_SQ, AVG(VAL_NUM)AS AVERAGE, STDEV(VAL_NUM) AS  ST_DEV, MIN(VAL_NUM) AS MINIMUM, MAX(VAL_NUM) AS MAXIMUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT
                        FROM
                        (
                        SELECT SMPL_NUM, R2.PROD_SQ, VAL_NUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT
                        FROM
                        (
                        SELECT SMPL_NUM, PROD_SQ, VAL_NUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
						
                        FROM
                        (
						
                        SELECT SMPLS.SMPL_NUM, PROD_SQ, RESULT.VAL_NUM, RESULT.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
                        
                        FROM
                        
						##PTNTB0073_30_SMPLS_FM1T084FASG SMPLS
                        
                        JOIN

                        (
                        SELECT T103.SMPL_NUM, T103.VAL_NUM, T103.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
                        FROM
                        (

                        SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
                        T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
                        FROM
                        dbo.MACT021_TST_STEP AS T021
                        INNER JOIN
                        dbo.MACT015_TST_SECT AS T015
                        ON
                        (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
                        AND
                        (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
                        AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
                        GROUP BY
                        T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
                        T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
                        HAVING

                        (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
                        AND((T021.STEP_DS) = @STEP_DS)
                        AND((T015.SECT_TTL_TXT) = @SECT_TTL_TXT))


                        ) AS TST_RSLT_FM1T085CASG_KEY_VALUES
                        INNER JOIN
                        dbo.MACT103_TSTRSLTSTP AS T103
                        ON
                        (TST_RSLT_FM1T085CASG_KEY_VALUES.TST_DFIN_STEP_NUM = T103.TST_DFIN_STEP_NUM)
                        AND
                        (TST_RSLT_FM1T085CASG_KEY_VALUES.TST_SECT_SQ = T103.TST_SECT_SQ)
                        AND
                        (TST_RSLT_FM1T085CASG_KEY_VALUES.TST_DFIN_VER_NUM = T103.TST_DFIN_VER_NUM)
                        AND(TST_RSLT_FM1T085CASG_KEY_VALUES.TST_DFIN_ID = T103.TST_DFIN_ID)
                        WHERE VAL_NUM IS NOT NULL
                        ) AS RESULT
                        ON SMPLS.SMPL_NUM = RESULT.SMPL_NUM
                        WHERE PROD_SQ IS NOT NULL
                        ) AS GET_30_RESULTS
                        
                        ) AS R2

                        JOIN


                        (SELECT T229.[PROD_SQ]
                                , T229.[VAL_AMT] - 0.05 AS LOWER_LIMIT, T229.[VAL_AMT] AS TARG, T229.[VAL_AMT] + 0.05 AS UPPER_LIMIT
                        FROM[MACDWSQL1].[dbo].[MACT229_PRDMXDDATA] AS T229
                        WHERE T229.PROD_DATA_TYP_CD = @PROD_DATA_TYP_CD AND VAL_AMT IS NOT NULL 
                        ) AS GET_GSB
                        ON
                        R2.PROD_SQ = GET_GSB.PROD_SQ
                        ) AS R3
                        GROUP BY PROD_SQ, TST_DFIN_ID,STEP_DS, SECT_TTL_TXT, LOWER_LIMIT,TARG , UPPER_LIMIT

                        ) AS R4

                        INNER JOIN
                        (
                        SELECT IIf([FDOT_MNG_DIST_CD] Is Null, [FDOT_GEOG_DIST_CD], [FDOT_MNG_DIST_CD]) AS DIST,
                            T079.FCLTY_ID, T070.FCLTY_DS, T225.PROD_CD, 
                        IIf([PRCS_NUM] Is Null,1, [PRCS_NUM]) AS PROC1, T225.PROD_NM, 
                        T226.MTRL_SQ, T226.PROD_DFIN_SQ, T226.FCLTY_SQ, 
                        T226.PROD_FCLTY_CLSF_CD, T226.MTRL_TYP_CD, T226.PROD_SQ
                        FROM
                        (dbo.MACT225_PROD_DFIN AS T225
                        INNER JOIN
                        dbo.MACT226_PRODUCT AS T226
                        ON 
                        (T225.PROD_DFIN_SQ = T226.PROD_DFIN_SQ)
                        AND
                        (T225.MTRL_SQ = T226.MTRL_SQ)) 
                        INNER JOIN
                        (dbo.MACT070_FCLTY AS T070
                        INNER JOIN
                        dbo.MACT079_PRODFCLTY AS T079
                        ON
                        T070.FCLTY_SQ = T079.FCLTY_SQ)
                        ON T226.FCLTY_SQ = T070.FCLTY_SQ
                        ) AS PRODUCTS
                        ON
                        R4.PROD_SQ = PRODUCTS.PROD_SQ
                        WHERE COUNT_SMPL >2 AND (PROD_CD LIKE @PROD_CD_C4 OR PROD_CD LIKE @PROD_CD_C5 OR PROD_CD LIKE @PROD_CD_F2) 	
                        DROP TABLE ##PTNTB0073_30_SMPLS_FM1T084FASG

            ";

                case 16:
                    return @"
                    --CREATE TEMP TABLE WITH ALL ****FM1T084FASG**** SAMPLES **********************************BEGIN
			DECLARE @TST_DFIN_ID VARCHAR(11)
SET @TST_DFIN_ID = 'FM1T084FASG';

IF OBJECT_ID('tempdb.dbo.##PTNTB0073_30_SMPLS_FM1T084FASG', 'U') IS NOT NULL
               DROP TABLE ##PTNTB0073_30_SMPLS_FM1T084FASG;
			SELECT   [PROD_SQ]
                    ,[SMPL_NUM]
					,TST_DFIN_ID
					INTO ##PTNTB0073_30_SMPLS_FM1T084FASG
            FROM
			(
			SELECT   [PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
					,TST_DFIN_ID
				    ,ROW_NUMBER() OVER (PARTITION BY [PROD_SQ] ORDER BY [PROD_SQ], [SMPL_DT] DESC) AS RowNum  FROM ##PTNTB0070_30_SMPLS --Assigns a row number for samples grouped by Product oredered by Date, so we can take the las 30 later on
					WHERE TST_DFIN_ID = @TST_DFIN_ID
			) AS MySamples
			WHERE RowNum <31
			
			CREATE NONCLUSTERED INDEX IDX_NC_PROD_SQ__SMPL_NUM ON ##PTNTB0073_30_SMPLS_FM1T084FASG(SMPL_NUM, PROD_SQ)
			--CREATE TEMP TABLE WITH ALL ****FM1T084FASG**** SAMPLES **********************************END
            ";

                                                      
                case 18:
                    return @"
                       DECLARE @TST_DFIN_ID VARCHAR(11)
SET @TST_DFIN_ID = 'FM1T085CASG';
DECLARE @STEP_DS VARCHAR(21)
SET @STEP_DS = 'Bulk Specific Gravity';
DECLARE @SECT_TTL_TXT VARCHAR(4)
SET @SECT_TTL_TXT = 'Main';
DECLARE @PROD_DATA_TYP_CD CHAR(1)
SET @PROD_DATA_TYP_CD = 'N';
DECLARE @PROD_CD_C4 VARCHAR(4)
SET @PROD_CD_C4 = '%C4%';
DECLARE @PROD_CD_C5 VARCHAR(4)
SET @PROD_CD_C5 = '%C5%';
DECLARE @PROD_CD_F2 VARCHAR(4)
SET @PROD_CD_F2 = '%F2%';

SELECT R4.PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, COUNT_SMPL, MINIMUM, AVERAGE, MAXIMUM, ROUND(CONVERT(FLOAT, ST_DEV),6) AS ST_DEV, CONVERT(VARCHAR(6), (CONVERT(FLOAT, LOWER_LIMIT))) AS LOWER_LIMIT, CONVERT(VARCHAR(6), (CONVERT(FLOAT, TARG))) AS TARG, UPPER_LIMIT,
                        1000 AS Z
                        FROM
                        (
                        SELECT COUNT(PROD_SQ) AS COUNT_SMPL, PROD_SQ, AVG(VAL_NUM)AS AVERAGE, STDEV(VAL_NUM) AS  ST_DEV, MIN(VAL_NUM) AS MINIMUM, MAX(VAL_NUM) AS MAXIMUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT
                        FROM
                        (
                        SELECT SMPL_NUM, R2.PROD_SQ, VAL_NUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, LOWER_LIMIT, TARG, UPPER_LIMIT
                        FROM
                        (
                        SELECT SMPL_NUM, PROD_SQ, VAL_NUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
						
                        FROM
                        (
                        SELECT SMPLS.SMPL_NUM, PROD_SQ, RESULT.VAL_NUM, RESULT.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
                        
                        FROM
                        
						##PTNTB0074_30_SMPLS_FM1T085CASG SMPLS

                        JOIN

                        (
                        SELECT T103.SMPL_NUM, T103.VAL_NUM, T103.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
                        FROM
                        (

                        SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
                        T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
                        FROM
                        dbo.MACT021_TST_STEP AS T021
                        INNER JOIN
                        dbo.MACT015_TST_SECT AS T015
                        ON
                        (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
                        AND
                        (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
                        AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
                        GROUP BY
                        T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
                        T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
                        HAVING

                        (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
                        AND((T021.STEP_DS) = @STEP_DS)
                        AND((T015.SECT_TTL_TXT) = @SECT_TTL_TXT))


                        ) AS TST_RSLT_FM1T085CASG_KEY_VALUES
                        INNER JOIN
                        dbo.MACT103_TSTRSLTSTP AS T103
                        ON
                        (TST_RSLT_FM1T085CASG_KEY_VALUES.TST_DFIN_STEP_NUM = T103.TST_DFIN_STEP_NUM)
                        AND
                        (TST_RSLT_FM1T085CASG_KEY_VALUES.TST_SECT_SQ = T103.TST_SECT_SQ)
                        AND
                        (TST_RSLT_FM1T085CASG_KEY_VALUES.TST_DFIN_VER_NUM = T103.TST_DFIN_VER_NUM)
                        AND(TST_RSLT_FM1T085CASG_KEY_VALUES.TST_DFIN_ID = T103.TST_DFIN_ID)
                        WHERE VAL_NUM IS NOT NULL
                        ) AS RESULT
                        ON SMPLS.SMPL_NUM = RESULT.SMPL_NUM
                        WHERE PROD_SQ IS NOT NULL
                        ) AS GET_30_RESULTS
                        
                        ) AS R2

                        JOIN


                        (SELECT T229.[PROD_SQ]
                                , T229.[VAL_AMT] - 0.05 AS LOWER_LIMIT, T229.[VAL_AMT] AS TARG, T229.[VAL_AMT] + 0.05 AS UPPER_LIMIT
                        FROM[MACDWSQL1].[dbo].[MACT229_PRDMXDDATA] AS T229
                        WHERE T229.PROD_DATA_TYP_CD = @PROD_DATA_TYP_CD AND VAL_AMT IS NOT NULL 
                        ) AS GET_GSB
                        ON
                        R2.PROD_SQ = GET_GSB.PROD_SQ
                        ) AS R3
                        GROUP BY PROD_SQ, TST_DFIN_ID,STEP_DS, SECT_TTL_TXT, LOWER_LIMIT,TARG , UPPER_LIMIT

                        ) AS R4

                        INNER JOIN
                        (
                        SELECT IIf([FDOT_MNG_DIST_CD] Is Null, [FDOT_GEOG_DIST_CD], [FDOT_MNG_DIST_CD]) AS DIST,
                            T079.FCLTY_ID, T070.FCLTY_DS, T225.PROD_CD, 
                        IIf([PRCS_NUM] Is Null,1, [PRCS_NUM]) AS PROC1, T225.PROD_NM, 
                        T226.MTRL_SQ, T226.PROD_DFIN_SQ, T226.FCLTY_SQ, 
                        T226.PROD_FCLTY_CLSF_CD, T226.MTRL_TYP_CD, T226.PROD_SQ
                        FROM
                        (dbo.MACT225_PROD_DFIN AS T225

                        INNER JOIN
                        dbo.MACT226_PRODUCT AS T226
                        ON 
                        (T225.PROD_DFIN_SQ = T226.PROD_DFIN_SQ)
                        AND
                        (T225.MTRL_SQ = T226.MTRL_SQ)) 
                        INNER JOIN
                        (dbo.MACT070_FCLTY AS T070
                        INNER JOIN
                        dbo.MACT079_PRODFCLTY AS T079
                        ON
                        T070.FCLTY_SQ = T079.FCLTY_SQ)
                        ON T226.FCLTY_SQ = T070.FCLTY_SQ
                        ) AS PRODUCTS
                        ON
                        R4.PROD_SQ = PRODUCTS.PROD_SQ
                        WHERE  COUNT_SMPL >2 AND (PROD_CD LIKE @PROD_CD_C4 OR PROD_CD LIKE @PROD_CD_C5 OR PROD_CD LIKE @PROD_CD_F2)
                        DROP TABLE ##PTNTB0074_30_SMPLS_FM1T085CASG

            ";

                case 19:
                    return @"--CREATE TEMP TABLE WITH ALL ****FM1T085CASG**** SAMPLES **********************************BEGIN

            DECLARE @TST_DFIN_ID VARCHAR(11)
SET @TST_DFIN_ID = 'FM1T085CASG';

IF OBJECT_ID('tempdb.dbo.##PTNTB0074_30_SMPLS_FM1T085CASG', 'U') IS NOT NULL
               DROP TABLE ##PTNTB0074_30_SMPLS_FM1T085CASG;
			SELECT[PROD_SQ]
                    ,[SMPL_NUM]
                    , TST_DFIN_ID

                    INTO ##PTNTB0074_30_SMPLS_FM1T085CASG
            FROM
            (
            SELECT[PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
                    , TST_DFIN_ID
                    , ROW_NUMBER() OVER(PARTITION BY[PROD_SQ] ORDER BY[PROD_SQ], [SMPL_DT] DESC) AS RowNum  FROM ##PTNTB0070_30_SMPLS --Assigns a row number for samples grouped by Product oredered by Date, so we can take the las 30 later on
					WHERE TST_DFIN_ID = @TST_DFIN_ID
            ) AS MySamples

            WHERE RowNum < 31


            CREATE NONCLUSTERED INDEX IDX_NC_PROD_SQ__SMPL_NUM ON ##PTNTB0074_30_SMPLS_FM1T085CASG(SMPL_NUM, PROD_SQ )

			--CREATE TEMP TABLE WITH ALL * ***FM1T085CASG * ***SAMPLES * *********************************END";
                    
                case 20:
                    return @"--CREATE TEMP TABLE WITH ALL ****FM1T096**** SAMPLES **********************************BEGIN
			
			DECLARE @TST_DFIN_ID VARCHAR(11)
            SET @TST_DFIN_ID = 'FM1T096';

            IF OBJECT_ID('tempdb.dbo.##PTNTB0075_30_SMPLS_FM1T096', 'U') IS NOT NULL
               DROP TABLE ##PTNTB0075_30_SMPLS_FM1T096;
			SELECT   [PROD_SQ]
                    ,[SMPL_NUM]
					,TST_DFIN_ID
					INTO ##PTNTB0075_30_SMPLS_FM1T096
            FROM
			(
			SELECT   [PROD_SQ]
                    ,[MTRL_TYP_CD]
                    ,[SMPL_NUM]
                    ,[SMPL_DT]
                    ,[STAT_CD]
					,TST_DFIN_ID
				    ,ROW_NUMBER() OVER (PARTITION BY [PROD_SQ] ORDER BY [PROD_SQ], [SMPL_DT] DESC) AS RowNum  FROM ##PTNTB0070_30_SMPLS  --Assigns a row number for samples grouped by Product oredered by Date, so we can take the las 30 later on
					WHERE TST_DFIN_ID = @TST_DFIN_ID
			) AS MySamples
			WHERE RowNum <31
			
			CREATE NONCLUSTERED INDEX IDX_NC_PROD_SQ__SMPL_NUM ON ##PTNTB0075_30_SMPLS_FM1T096(SMPL_NUM, PROD_SQ )

			--CREATE TEMP TABLE WITH ALL ****FM1T096**** SAMPLES **********************************END";

                case 21:
                    return @"
                        DECLARE @TST_DFIN_ID VARCHAR(11)
SET @TST_DFIN_ID = 'FM1T096';
DECLARE @STEP_DS VARCHAR(21)
SET @STEP_DS = 'Percent Loss By Abrasion';
DECLARE @SECT_TTL_TXT VARCHAR(4)
SET @SECT_TTL_TXT = 'Charges';


SELECT PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, COUNT_SMPL, MINIMUM, AVERAGE, MAXIMUM, ST_DEV,  '-' AS LOWER_LIMIT, '-' AS TARG, UPPER_LIMIT,
                         Z
						FROM
						(
                        SELECT RESULTS.PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT, COUNT_SMPL,  ROUND(CONVERT(FLOAT,MINIMUM),3) AS MINIMUM, ROUND(CONVERT(FLOAT,AVERAGE),3) AS AVERAGE, ROUND(CONVERT(FLOAT, MAXIMUM),3) AS  MAXIMUM, ROUND(CONVERT(FLOAT,ST_DEV),3) AS ST_DEV,  '-' AS LOWER_LIMIT, '-' AS TARG,RIGHT(LIMITS.TRGT_VAL,2) AS UPPER_LIMIT,
                        ROUND(CONVERT(FLOAT, ABS(IIF(((ST_DEV = 0) OR(ST_DEV IS NULL)), AVERAGE, (RIGHT(LIMITS.TRGT_VAL,2) - AVERAGE) / ST_DEV))), 3) AS Z
                        FROM
                        (SELECT COUNT(SMPL_NUM) AS COUNT_SMPL, R2.PROD_SQ, MIN(VAL_NUM) AS MINIMUM, AVG(VAL_NUM)AS AVERAGE, MAX(VAL_NUM) AS  MAXIMUM, STDEV(VAL_NUM) AS ST_DEV, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT

                        FROM
                        (
                        SELECT SMPL_NUM, PROD_SQ, VAL_NUM, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
						
                        FROM
                        (
                        SELECT SMPLS.SMPL_NUM, PROD_SQ, RESULT.VAL_NUM, RESULT.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
                        
                        FROM
						
                        ##PTNTB0075_30_SMPLS_FM1T096 SMPLS

                        JOIN

                        (
                        SELECT T103.SMPL_NUM, T103.VAL_NUM, T103.TST_DFIN_ID, STEP_DS, SECT_TTL_TXT
                        FROM
                        (

                        SELECT T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
                        T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
                        FROM
                        dbo.MACT021_TST_STEP AS T021
                        INNER JOIN
                        dbo.MACT015_TST_SECT AS T015
                        ON
                        (T021.TST_SECT_SQ = T015.TST_SECT_SQ)
                        AND
                        (T021.TST_DFIN_VER_NUM = T015.TST_DFIN_VER_NUM)
                        AND(T021.TST_DFIN_ID = T015.TST_DFIN_ID)
                        GROUP BY
                        T021.TST_DFIN_ID, T021.STEP_DS, T015.SECT_TTL_TXT,
                        T015.TST_DFIN_VER_NUM, T015.TST_SECT_SQ, T021.TST_DFIN_STEP_NUM
                        HAVING

                        (((T021.TST_DFIN_ID) = @TST_DFIN_ID)
                        AND((T021.STEP_DS) = @STEP_DS)
                        AND((T015.SECT_TTL_TXT) = @SECT_TTL_TXT))



                        ) AS TST_RSLT_FM1T096_KEY_VALUES
                        INNER JOIN
                        dbo.MACT103_TSTRSLTSTP AS T103
                        ON
                        (TST_RSLT_FM1T096_KEY_VALUES.TST_DFIN_STEP_NUM = T103.TST_DFIN_STEP_NUM)
                        AND
                        (TST_RSLT_FM1T096_KEY_VALUES.TST_SECT_SQ = T103.TST_SECT_SQ)
                        AND
                        (TST_RSLT_FM1T096_KEY_VALUES.TST_DFIN_VER_NUM = T103.TST_DFIN_VER_NUM)
                        AND(TST_RSLT_FM1T096_KEY_VALUES.TST_DFIN_ID = T103.TST_DFIN_ID)
                        WHERE VAL_NUM IS NOT NULL
                        ) AS RESULT
                        ON SMPLS.SMPL_NUM = RESULT.SMPL_NUM
                        WHERE PROD_SQ IS NOT NULL
                        ) AS GET_30_RESULTS
                        
                        ) AS R2
                        GROUP BY PROD_SQ, TST_DFIN_ID, STEP_DS, SECT_TTL_TXT

                        ) AS RESULTS

                        JOIN

                                            
                       ##PTNTB0076_FM1T096_LIMITS  AS LIMITS

                            ON LIMITS.PROD_SQ = RESULTS.PROD_SQ
                            WHERE COUNT_SMPL>2 AND ST_DEV IS NOT NULL AND ST_DEV <> 0
                            ) AS MY_RESULTS
                            

                        ORDER BY PROD_SQ

			

            ";

                case 22:
                    return @"
            DECLARE @MTRL_TYP_CD VARCHAR(6)
SET @MTRL_TYP_CD = 'AGGREG';
DECLARE @TST_DFIN_ID VARCHAR(11)
SET @TST_DFIN_ID = 'FM1T096';

IF OBJECT_ID('tempdb.dbo.##PTNTB0076_FM1T096_LIMITS', 'U') IS NOT NULL
               DROP TABLE ##PTNTB0076_FM1T096_LIMITS;
			SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.SPEC_SQ
 INTO ##PTNTB0076_FM1T096_LIMITS --We store temporarily All LIMITS FOR FM1T096 
  FROM
                        (SELECT [TST_DFIN_ID]
                                ,[TST_DFIN_STEP_NUM]
                                ,[SPEC_SQ]
                                ,[SPEC_CAT_SQ]
                                ,[PROD_SQ]
                                ,[TRGT_VAL]
                                ,[COND_VAL]

                            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] 
                            AS TRGTS


                            WHERE TST_DFIN_VER_NUM = (

                            SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
                            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
                            WHERE TST_DFIN_ID = @TST_DFIN_ID)
                            AND TST_DFIN_ID = @TST_DFIN_ID
                            AND SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NOT NULL) AS A

                            --*************************************************************************************************************(END A)
                            RIGHT JOIN 

                            --************************************************************************************************************(BEGIN B)
                        ( SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM
                        (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ --, T006.SPEC_CAT_SQ, T006.SPEC_SQ
                        FROM
                        MACT006_SPECCAT AS T006
                        WHERE PROD_DFIN_SQ IS NOT NULL
                        GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
                        ) AS SC

                        JOIN


                        (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

                        FROM
                        (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
                        FROM
                        MACT226_PRODUCT AS T226
                        WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
                        JOIN

                        (SELECT T225.PROD_DFIN_SQ
                        FROM
                        MACT225_PROD_DFIN AS T225) AS P2
                        ON
                        P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
                        ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ) AS B
                            --************************************************************************************************************(END B)
                            ON A.SPEC_SQ = B.SPEC_SQ
                            WHERE TRGT_VAL IS NOT NULL

                            --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                            UNION ALL

                            --***************************************************SPEC_CAT_SQ IS NOT NULL AND SPEC_SQ IS NOT NULL********************(BEGING A)     B.SPEC_SQ, B.SPEC_CAT_SQ, A.SPEC_SQ, A.SPEC_CAT_SQ, 
                        SELECT DISTINCT B.PROD_SQ,A.TRGT_VAL, A.SPEC_SQ FROM
                        (SELECT [TST_DFIN_ID]
                                ,[TST_DFIN_STEP_NUM]
                                ,[SPEC_SQ]
                                ,[SPEC_CAT_SQ]
                                ,[PROD_SQ]
                                ,[TRGT_VAL]
                                ,[COND_VAL]
                            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] 
                            AS TRGTS


                            WHERE TST_DFIN_VER_NUM = (

                            SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
                            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
                            WHERE TST_DFIN_ID = @TST_DFIN_ID)
                            AND TST_DFIN_ID = @TST_DFIN_ID
                            AND SPEC_CAT_SQ IS NOT NULL AND SPEC_SQ IS NOT NULL) AS A

                            --*************************************************************************************************************(END A)
                            RIGHT JOIN 

                            --************************************************************************************************************(BEGIN B)
                        ( SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM
                        (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ --, T006.SPEC_CAT_SQ, T006.SPEC_SQ
                        FROM
                        MACT006_SPECCAT AS T006
                        WHERE PROD_DFIN_SQ IS NOT NULL
                        GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
                        ) AS SC

                        JOIN


                        (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

                        FROM
                        (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
                        FROM
                        MACT226_PRODUCT AS T226
                        WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
                        JOIN

                        (SELECT T225.PROD_DFIN_SQ
                        FROM
                        MACT225_PROD_DFIN AS T225) AS P2
                        ON
                        P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
                        ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ) AS B
                            --************************************************************************************************************(END B)
                            ON A.SPEC_SQ = B.SPEC_SQ AND A.SPEC_CAT_SQ = B.SPEC_CAT_SQ
                            WHERE TRGT_VAL IS NOT NULL

                            --+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                            UNION ALL

                            --***************************************************SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NULL********************(BEGING A) B.SPEC_SQ, B.SPEC_CAT_SQ, A.SPEC_SQ, A.SPEC_CAT_SQ, 
                        SELECT DISTINCT B.PROD_SQ, A.TRGT_VAL, A.SPEC_SQ FROM
                        (SELECT [TST_DFIN_ID]
                                ,[TST_DFIN_STEP_NUM]
                                ,[SPEC_SQ]
                                ,[SPEC_CAT_SQ]
                                ,[PROD_SQ]
                                ,[TRGT_VAL]
                                ,[COND_VAL]
                            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView] 
                            AS TRGTS


                            WHERE TST_DFIN_VER_NUM = (

                            SELECT MAX(TST_DFIN_VER_NUM) AS TST_DFIN_VER_NUM
                            FROM [MACDWSQL1].[dbo].[MACT603_TargetAndLimitView]
                            WHERE TST_DFIN_ID = @TST_DFIN_ID)
                            AND TST_DFIN_ID = @TST_DFIN_ID
                            AND SPEC_CAT_SQ IS NULL AND SPEC_SQ IS NULL) AS A

                            --*************************************************************************************************************(END A)
                            RIGHT JOIN 

                            --************************************************************************************************************(BEGIN B)
                        (  SELECT P.PROD_SQ, SC.SPEC_SQ, SC.SPEC_CAT_SQ FROM
                        (SELECT T006.PROD_DFIN_SQ, T006.SPEC_SQ, MAX(T006.SPEC_CAT_SQ) AS SPEC_CAT_SQ --, T006.SPEC_CAT_SQ, T006.SPEC_SQ
                        FROM
                        MACT006_SPECCAT AS T006
                        WHERE PROD_DFIN_SQ IS NOT NULL
                        GROUP BY PROD_DFIN_SQ, T006.SPEC_SQ
                        ) AS SC

                        LEFT JOIN

						
                        (SELECT DISTINCT P1.PROD_DFIN_SQ, P1.PROD_SQ

                        FROM
                        (SELECT T226.PROD_DFIN_SQ, T226.MTRL_TYP_CD, T226.PROD_SQ
                        FROM
                        MACT226_PRODUCT AS T226
                        WHERE T226.MTRL_TYP_CD = @MTRL_TYP_CD) AS P1
                        JOIN
												
                        MACT225_PROD_DFIN  AS P2
                        
                        ON
                        P1.PROD_DFIN_SQ = P2.PROD_DFIN_SQ) AS P
                        ON P.PROD_DFIN_SQ = SC.PROD_DFIN_SQ ) AS B
                            --************************************************************************************************************(END B)
                            ON A.PROD_SQ = B.PROD_SQ
                            WHERE TRGT_VAL IS NOT NULL

							CREATE CLUSTERED INDEX IDX_C_ ON ##PTNTB0076_FM1T096_LIMITS (PROD_SQ)
            ";
                    
                case 23:
                    return @"INSERT INTO AGGREG.PTNTB0016_COMPLIANCE_UPDATE([ID],[UPDT_DATE]) 
                        VALUES((IIF((Select MAX(ID)from AGGREG.PTNTB0016_COMPLIANCE_UPDATE) IS NOT NULL, (Select MAX(ID)from AGGREG.PTNTB0016_COMPLIANCE_UPDATE) + 1 , 1)),GETDATE());";
                    
                case 24:
                    return @"SELECT [EMAIL_BODY] AS body, IIF([EVENT_ID] <99, '0' + CONVERT(VARCHAR(2),[EVENT_ID]), '99') AS district
              FROM [AGGREG].[PTNTB0018_EMAIL_BODIES]
              WHERE CONVERT(date, [UPDT_TMS])= CONVERT(date, GETDATE())
            ";

                default:
                    return "My Query";
            }
        }
    }
}
