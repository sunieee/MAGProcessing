from utils import *
import pandas as pd


timeseries_df = pd.read_sql_query(f"""select citeStartYear, citeEndYear, totalCitationCount, citationCountByYear, paperID 
                                  from {citation_timeseries_table}
                                  where paperID in {nodes}""", engine)


row = edges.iloc[1]
authorID = row['authorID']
citing = row['citingpaperID']
cited = row['citedpaperID']

citingRow = timeseries_df.loc[timeseries_df['paperID'] == citing].values.tolist()
citedRow = timeseries_df.loc[timeseries_df['paperID'] == cited].values.tolist()