import pandas as pd
import os

fieldName = 'visualization'
prob_dir = '/home/sy/GeneticFlowVis/csv/visualization'


match_df = pd.read_csv('../merge_author/out/match.csv', index_col=0)

for i in list(match_df.index)[::-1]:
    id1 = match_df.loc[i]['id1']
    id2 = match_df.loc[i]['id2']
    name1 = match_df.loc[i]['name1']
    name2 = match_df.loc[i]['name2']
    r1 = match_df.loc[i]['r1']
    r2 = match_df.loc[i]['r2']

    authorTableName = "".join(filter(str.isalpha, name1)).lower() + str(r1)
    print('=' * 20)
    print(f'merging authors: {name1}({id2}) -> {name2}({id1})')

    link_path = f'data/csv/{fieldName}/links_{authorTableName}.csv'
    links_df = pd.read_csv(link_path)
    prob_df1 = pd.read_csv(os.path.join(prob_dir, f'links_{r1}.csv'))
    prob_df2 = pd.read_csv(os.path.join(prob_dir, f'links_{r2}.csv'))

    # merge df1 and df2
    prob_df = pd.concat([prob_df1, prob_df2], ignore_index=True)

    # links_df (citingpaperID, citedpaperID) -> prob_df (childrenID, parentID), set extendsProb with the latter
    def get_extends_prob(row):
        match = prob_df.loc[(prob_df['childrenID'] == row['citingpaperID']) & (prob_df['parentID'] == row['citedpaperID'])]['extendsProb']
        return match.values[0] if not match.empty else -1

    links_df['extends_prob'] = links_df.apply(get_extends_prob, axis=1)

    links_df.to_csv(link_path, index=False)