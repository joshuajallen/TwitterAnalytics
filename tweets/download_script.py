import get_tweets as t
import pandas as pd
import pyodbc
api = t.get_api()
t.get_backlog(api = api)
t.get_all_tweets(api = api)
print("getting tweet numbers")
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=SQLDV-DW-SQL02\inst1;Database=DITO;Trusted_Connection=yes')
cursor = conn.cursor()
cursor.execute("exec PMM_stg.TweetsPerHour")
cursor.execute("exec PMM_stg.TweetsToTableau")
conn.commit()
cursor.close()
conn.close()

print("getting tweet words")
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=SQLDV-DW-SQL02\inst1;Database=DITO;Trusted_Connection=yes')
cursor = conn.cursor()
cursor.execute('exec PMM_stg.Insert_Trending')
conn.commit()

cursor.execute("exec DITO_Maintenance.DB_Maintenance")
conn.commit()

cursor.close()
conn.close()
