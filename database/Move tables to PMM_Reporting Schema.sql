USE DITO
GO

/*
Script to shift the PMM user facing tables over to the PMM_Stg schema
Only needs running once
*/


ALTER SCHEMA PMM_Reporting TRANSFER [dbo].[Tweets_Tableau];  
GO 



ALTER SCHEMA PMM_Reporting TRANSFER [dbo].[Tweets_Tableau_Hour];  
GO 


ALTER SCHEMA PMM_Reporting TRANSFER [dbo].[Tweets_Tableau_Word];  
GO 


ALTER SCHEMA PMM_Reporting TRANSFER [dbo].[Tweets_Rumours];  
GO 