from utils import *

# dump all top field authors
cursor.execute(f"""
select * from scigene_{fieldName}_field.authors_field
    where authorRank <= {numOfTopAuthors} or {filterCondition}
    INTO OUTFILE 'data/csv/{fieldName}/top_field_authors.csv' 
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n';""")

# select exact top field authors
cursor.execute(f"""
select authorID, name, authorRank, PaperCount_field 
    from scigene_{fieldName}_field.authors_field
    where {filterCondition};""")
rows = cursor.fetchall()

# process each author
for row in rows:
    authorID = str(row[0].strip())
    authorName = str(row[1].strip())
    rank = int(row[2])

    authorTableName = "".join(filter(str.isalpha, authorName)).lower() + str(rank)

    execute(f"""
select * from papers_arc_{authorTableName} 
    INTO OUTFILE 'data/csv/{fieldName}/papers_arc_{authorTableName}.csv' 
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n';

select * from influence_arc_{authorTableName} 
    INTO OUTFILE 'data/csv/{fieldName}/influence_arc_{authorTableName}.csv' 
    FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n';
""")

    print(f"Dump papers and links for field author {authorName} with rank {rank}: {authorTableName}")

cursor.close()
conn.close()
