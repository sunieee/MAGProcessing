from utils import *

old_node_dir = f'data/old_{fieldName}'
rank_df = pd.read_csv(f'{old_node_dir}/top_field_authors.csv', names=
                      ["authorID", "rank", "name", "PaperCount", "CitationCount", "PaperCount_field", "authorRank", "CitationCount_field", "hIndex_field", "FellowType"])

authorID2rank = dict(zip(rank_df['authorID'], rank_df['authorRank']))

for f in os.listdir(node_dir):
    if f.startswith('papers_'):
        authorID = int(f.split('_')[-1].split('.')[0])

        df = pd.read_csv(os.path.join(node_dir, f))

        old_df = pd.read_csv(f'{old_node_dir}/papers_{authorID2rank[authorID]}.csv', index_col=0)
        columns = old_df.columns

        df = df.merge(old_df, left_on=['paperID'], right_on=['paperID'], how='left', suffixes=('', '_old'))
        print(df.columns)
        df = df[columns]

        print(df.head())

        df.to_csv(os.path.join(node_dir, f), index=False)