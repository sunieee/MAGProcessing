import pandas as pd
import time
from utils import *


for f in os.listdir(node_dir):
    if f.startswith('links_'):
        authorID = f.split('_')[-1].split('.')[0]
        print('='*20, authorID, '='*20)

        df = pd.read_csv(os.path.join(node_dir, f))
        df['citingpaperID'] = df['citingpaperID'].astype(str)
        df['citedpaperID'] = df['citedpaperID'].astype(str)
        
        selected_paper_author = pd.read_sql_query(f"""select * from paper_author_field where authorID='{authorID}'""", engine)
        selected_paper = selected_paper_author['paperID'].drop_duplicates().values.tolist()
        selected_edge_df = edge_df[edge_df['authorID'] == authorID]
        print('links', len(df)) 
        print('selected_paper', len(selected_paper))
        print('selected edge', len(selected_edge_df))
        # links_xxx怎么生成的，有sharedAuthors 但是作者并非本人
        # SELECT * FROM MACG.paper_author where authorID='2003408012';

        df = df[df["citingpaperID"].isin(selected_paper) & df["citedpaperID"].isin(selected_paper)]
        
        assert len(df) == len(selected_edge_df)
        df = df.merge(selected_edge_df, 
                      left_on=['citingpaperID', 'citedpaperID'], 
                      right_on=['citingpaperID', 'citedpaperID'], how='left')
        df['extendsProb'] = df['proba'].fillna('\\N')
        df = df[['citingpaperID', 'citedpaperID', 'extendsProb']]
        df.columns = ['childrenID', 'parentID', 'extendsProb']
        df['citationContext'] = None
        # print(df.head())
        

        for i in range(len(df)//10):
            citing = df.iloc[i]['childrenID']
            cited = df.iloc[i]['parentID']
            result = executeFetch(
                f"select citation_context from CitationContextContent where citingpaperID='{citing}' and citedpaperID='{cited}'", 
                cursor=cursor118)
            if result:
                df.iloc[i]['citationContext'] = ' '.join(result[0])

        print(df.head())
        # df.to_csv(f"link_{authorID}.csv", index=False)

