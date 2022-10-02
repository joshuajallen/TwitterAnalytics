USE DITO
GO

/*
Script to shift the PMM stored procs over to the PMM_Stg schema
Only needs running once
*/


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Find_Single_Words];  
GO 



ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Insert_Rumours];  
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Insert_Trending];  
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Insert_Tweet_Text];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Insert_Words];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweet_Calc_TFIDF];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweet_Medians];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[TweetsPerHour];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[TweetsToTableau];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[usp_ReTweetsByHourFirmCat];
GO 