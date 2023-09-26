from utils import *
import pandas as pd

# dump all top field authors
df = pd.read_sql(f"""select * from scigene_{fieldName}_field.authors_field
    where authorRank <= {numOfTopAuthors} or {filterCondition}""", conn)
df.to_csv(f'data/csv/{fieldName}/top_field_authors.csv', index=False)

# select exact top field authors
rows = executeFetch(f"""
select authorID, name, authorRank, PaperCount_field 
    from scigene_{fieldName}_field.authors_field
    where {filterCondition};""")

# process each author
for row in rows:
    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    rank = int(row[2])

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    papers_df = pd.read_sql(f"SELECT * FROM papers_{authorTableName}", conn)
    links_df = pd.read_sql(f"SELECT * FROM links_{authorTableName}", conn)

    papers_df.to_csv(f'data/csv/{fieldName}/papers_{authorTableName}.csv', index=False)
    links_df.to_csv(f'data/csv/{fieldName}/links_{authorTableName}.csv', index=False)

    print(f"Dump papers and links for field author {authorName} with rank {rank}: {authorTableName}")

cursor.close()
conn.close()
