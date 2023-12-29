import pandas as pd
import pymysql
import datetime
import os


def create_connection():
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db='MACG',
                            charset='utf8')
    return conn, conn.cursor()


conn, cursor = create_connection()
df_authors = pd.read_csv('out/authors.csv')
df_authors['authorID'] = df_authors['authorID'].astype(str)
df_authors['compareAuthorID'] = df_authors['compareAuthorID'].astype(str)

authorID_list = set(df_authors['authorID'].tolist())
authorID_str = ','.join([f'\'{x}\'' for x in authorID_list])

print('loading data from database', datetime.datetime.now().strftime("%H:%M:%S"))
path = f"out/csv"
if not os.path.exists(path):
    os.makedirs(path)
    df_paper_author = pd.read_sql_query(f"select * from paper_author where authorID in ({authorID_str})", conn)
    df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
    paperID_list = set(df_paper_author['paperID'].tolist())
    paperID_str = ','.join([f'\'{x}\'' for x in paperID_list])
    #
    df_papers = pd.read_sql_query(f"select * from papers where paperID in ({paperID_str})", conn)
    df_papers.to_csv(f"{path}/papers.csv", index=False)
    #
    df_paper_reference = pd.concat([
        pd.read_sql_query(f"select * from paper_reference where citingpaperID in ({paperID_str})", conn),
        pd.read_sql_query(f"select * from paper_reference where citedpaperID in ({paperID_str})", conn)
    ])
    df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
    df_paper_reference = None
else:
    df_paper_author = pd.read_csv(f"{path}/paper_author.csv")
    df_papers = pd.read_csv(f"{path}/papers.csv")
    #
    df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
    df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
    df_papers['paperID'] = df_papers['paperID'].astype(str)

df_papers['PublicationDate'] = pd.to_datetime(df_papers['PublicationDate'])
df_authors = None
