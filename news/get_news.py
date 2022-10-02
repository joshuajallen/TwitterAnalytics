from bs4 import BeautifulSoup
import re
import pandas as pd
import pyodbc
import subprocess
from datetime import datetime

def get_web_page(link):
    return subprocess.check_output("rscript --vanilla get_web_page.r " + link)
    
title_re = re.compile("(?<=Google Alert - )[^<]+")
http_re = re.compile("^https?://(www\.)?")
domain_re = re.compile("^[^/]+")
bold_tag_re = re.compile("<b>|</b>")

def domain(link):
    link = http_re.sub("", link)
    domain = domain_re.findall(link)
    return domain[0]

def get_alerts(page, firm, sub_term):
    soup = BeautifulSoup(page, 'lxml')

    query = title_re.findall(soup.find_all("title")[0].get_text())

    sub_term_re = re.compile(sub_term)

    content = [bold_tag_re.sub("", c.get_text()) for c in soup.find_all("content")]

    tag = [i.get_text().split("feed:")[-1] for i in soup.find_all('id')]

    title = [bold_tag_re.sub("", t.get_text()) for t in soup.find_all('title')]

    published = [p.get_text() for p in soup.find_all('published')]

    link = [l['href'].split('a=t&url=')[-1] for l in soup.find_all('link', href = True)]

    source = [domain(l) for l in link]

    if len(tag) > 0:
        tag.pop(0)
        title.pop(0)
        link.pop(0)
        source.pop(0)

    firm_in_title = [len(sub_term_re.findall(s.lower())) > 0 for s in title]

    firm = [firm] * len(tag)
    query_string = query * len(tag)

    df = pd.DataFrame({'story_id': tag, 'firm': firm, 'title': title, 'link': link, 'published': published, 'snippet': content, 'source': source, 'firm_in_title': firm_in_title, 'query_string': query_string})

    return df

connection_string = 'DRIVER={SQL Server};SERVER=SQLDV-DW-SQL02\inst1;Database=DITO;Trusted_Connection=yes'

def to_sql(df):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute('truncate table pmm_stg.news_insert')
    
    for _, row in df.iterrows():
        cursor.execute('insert into pmm_stg.news_insert(story_id, firm, title, link, published, snippet, source, query_string, firm_in_title) values (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        row.story_id, row.firm, row.title, row.link, row.published, row.snippet, row.source, row.query_string, row.firm_in_title)

    conn.commit()
    cursor.execute('exec pmm_stg.insert_news')
    conn.commit()
    cursor.close()
    conn.close()

def get_metadata():
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    rows = cursor.execute('select * from pmm_stg.news_metadata')

    firm_list = rows.fetchall()

    cursor.close()
    conn.close()
    return firm_list

def update_data():
    start_filename = datetime.strftime(datetime.now(), 'logs/%Y%m%d_%H%M%S.txt')
    with open(start_filename, 'w') as f:
        f.write("Starting at " + datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S'))

        f.write("\n\nGetting metadata: ")

    firm_list = get_metadata()

    with open(start_filename, 'a') as f:
        f.write("Success\n\nGetting Firms:\n\n")

    full_df = None

    for firm, query_string, alert_link, sub_term in firm_list:
        with open(start_filename, 'a') as f:
            f.write(firm + "\n")
        page = get_web_page(alert_link)
        alert_df = get_alerts(page, firm = firm, sub_term = sub_term)
        full_df = pd.concat((full_df, alert_df))

    full_df = full_df.reset_index()

    with open(start_filename, 'a') as f:
        f.write("\nUploading data: ")

    full_df.to_csv(datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S') + ".csv", sep = "\t", index = False)

    to_sql(full_df)

    with open(start_filename, 'a') as f:
        f.write("Success\n\n")

        f.write("Finished at " + datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S'))
    

update_data()