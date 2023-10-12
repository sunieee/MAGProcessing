from utils import *
import pandas as pd

# dump all top field authors
df = pd.read_sql(f"""select * from {database}.authors_field
    where {filterCondition}""", conn)
df.to_csv(f'data/{database}/top_field_authors.csv', index=False)

# process each author
print('## start to process each author (download tables)', len(authors_rows))
count = 0
for row in tqdm(authors_rows):
    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    rank = int(row[2])
    count += 1
    print(f'### ({count}/{len(authors_rows)})', authorID, authorName, rank)

    # authorID = "".join(filter(str.isalpha, authorName)).lower() + str(rank)
    papers_df = pd.read_sql(f"SELECT * FROM papers_{authorID}", conn)
    papers_df.to_csv(f'data/{database}/papers/{authorID}.csv', index=False)

    print(f"Dump papers for field author {authorName} with rank {rank}: {authorID}")

cursor.close()
conn.close()
