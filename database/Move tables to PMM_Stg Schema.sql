USE DITO
GO

/*
Script to shift the PMM non-user facing tables over to the PMM_Stg schema
Only needs running once
*/


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[latest_ids];  
GO 



ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets];  
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Hour];  
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Median];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Tableau_Hour_Long];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Insert];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Joined];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Text];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Text_Insert];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_TFIDF];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Trending];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Word];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Word_New];
GO


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Word_Upload];
GO  


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Metadata];
GO 


ALTER SCHEMA PMM_Stg TRANSFER [dbo].[Tweets_Single_Word];
GO