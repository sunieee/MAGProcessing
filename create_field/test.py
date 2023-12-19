from utils import *
from tqdm import tqdm


with open(f'out/{database}/authorID2h_index.json', 'r') as f:
    authorID2h_index = json.load(f)

# cursor.executemany(update_query, h_index_updates)
# conn.commit()

for authorID, h_index in tqdm(authorID2h_index.items()):
    cursor.execute(f"UPDATE authors_field SET hIndex_field={h_index} WHERE authorID=\'{authorID}\'")
conn.commit()
