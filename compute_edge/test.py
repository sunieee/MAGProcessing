import pandas as pd
from utils import *


df = pd.read_csv('all_features.csv')
# set index: (citingpaperID, citedpaperID)
df = df.set_index(['citingpaperID', 'citedpaperID'])


for authorID in authorID_list:
    print('=' * 20, authorID, '=' * 20)
    selected_paper_author = pd.read_sql_query(f"""select * from paper_author_field where authorID='{authorID}'""", engine)
    selected_paper = selected_paper_author[['paperID']].drop_duplicates()

    selected_edges = pd.read_sql_query(f"""select * from paper_reference_field
        where citingpaperID in {tuple(selected_paper['paperID'])}
        and citedpaperID in {tuple(selected_paper['paperID'])}""", engine) 
    print(len(selected_edges))

    not_found_edges = []

    for i, row in selected_edges.iterrows():
        citingpaperID = row['citingpaperID']
        citedpaperID = row['citedpaperID']
        # print(citingpaperID, citedpaperID)
        
        if (citingpaperID, citedpaperID) in df.index:
            df.loc[(citingpaperID, citedpaperID), 'reference'] = 1
        else:
            # print(f'edge not found: {citingpaperID} -> {citedpaperID}')
            not_found_edges.append((citedpaperID, citedpaperID))

    print(len(not_found_edges))
    