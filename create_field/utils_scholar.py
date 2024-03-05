import pandas as pd
import pymysql
from datetime import datetime
import os
from tqdm import tqdm


def create_connection(database='MACG'):
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    return conn, conn.cursor()

field = os.environ.get('field')
database = os.environ.get('database', 'MACG')

conn, cursor = create_connection()
if os.path.exists(f'out/{field}/authors.csv'):
    df_authors = pd.read_csv(f'out/{field}/authors.csv')
else:
    df_authors = pd.read_csv(f'out/{field}/authors_all.csv')

df_authors['authorID'] = df_authors['authorID'].astype(str)

authorID_list = set(df_authors['authorID'].tolist())
authorIDs = authorID_list
authorID_str = ','.join([f'\'{x}\'' for x in authorID_list])

print('loading data from database', datetime.now().strftime("%H:%M:%S"))
path = f'out/{field}/csv'
if not os.path.exists(path):
    os.makedirs(path)
    if database == 'MACG':
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
        df_paper_author = pd.read_csv(f"out/{database}/csv/paper_author.csv")
        df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
        df_paper_author = df_paper_author[df_paper_author['authorID'].isin(authorID_list)]
        df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
        paperIDs = set(df_paper_author['paperID'].tolist())

        df_papers = pd.read_csv(f"out/{database}/csv/papers.csv")
        df_papers = df_papers[df_papers['paperID'].isin(paperIDs)]
        df_papers.to_csv(f"{path}/papers.csv", index=False)

        df_paper_reference = pd.read_csv(f"out/{database}/csv/paper_reference.csv")
        df_paper_reference = df_paper_reference[df_paper_reference['citingpaperID'].isin(paperIDs) | df_paper_reference['citedpaperID'].isin(paperIDs)]
        df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
        df_paper_reference = None
else:
    df_paper_author = pd.read_csv(f"{path}/paper_author.csv")
    df_papers = pd.read_csv(f"{path}/papers.csv")
    
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
df_papers['paperID'] = df_papers['paperID'].astype(str)


if os.environ.get('scholar') == '1' and not os.path.exists(f'out/{field}/authors.csv'):
    typ = 10    # turing
    award_df = pd.DataFrame(columns=['original_author_name', 'year', 'type', 'MAGID', 'ARCID'])
    for row in df_authors.iterrows():
        row = row[1]
        award_df.loc[len(award_df)] =[row['name'], row['year'], typ, row['authorID'], 'NULL']
    award_df.to_csv(f'out/{field}/award_authors{typ}.csv', index=False)


    for original in tqdm(set(df_authors['original'].to_list())):
        df_authors_original = df_authors[df_authors['original'] == original].copy()
        if len(df_authors_original) == 1:
            continue
        print("merge authors:", original, len(df_authors_original))
        firstAuthorID = df_authors_original.iloc[0]['authorID']
        firstPaperCount = df_authors_original.iloc[0]['PaperCount']
        firstCitationCount = df_authors_original.iloc[0]['CitationCount']
        for row in df_authors_original.to_dict('records')[1:]:
            authorID = row['authorID']
            df_paper_author.loc[df_paper_author['authorID'] == authorID, 'authorID'] = firstAuthorID
            firstCitationCount += row['CitationCount']
            firstPaperCount += row['PaperCount']
        df_authors.loc[df_authors['original'] == original, 'PaperCount'] = firstPaperCount
        df_authors.loc[df_authors['original'] == original, 'CitationCount'] = firstCitationCount
    
    df_authors.drop_duplicates(subset=['original'], inplace=True, keep='first')
    df_authors.to_csv(f'out/{field}/authors.csv',index=False)
    df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)


if 'PaperCount_field' not in df_authors.columns:
    paper_count = df_paper_author.groupby('authorID')['paperID'].count().reset_index(name='PaperCount_field')
    df_authors = df_authors.merge(paper_count, on='authorID', how='left')
    df_authors['PaperCount_field'] = df_authors['PaperCount_field'].fillna(0)
    df_authors.to_csv(f'out/{field}/authors.csv',index=False)

df_papers['PublicationDate'] = pd.to_datetime(df_papers['PublicationDate'])
df_papers['year'] = df_papers['PublicationDate'].dt.year

