"""
写一个python程序，将下面查询的结果插入到 scigene_visualization_field.paper_author_filed 表格中
use MACG;
select paperID, authorID, authorOrder from papers_field as A 
    join paper_author as B 
    where B.authorID='3206897746' and A.paperID=B.paperID and 
    fieldID in (159384605, 36464697, 4379982, 83893533);
"""

from utils import *

# Execute the SELECT query
select_query = """
SELECT A.paperID as paperID, authorID, authorOrder 
FROM MACG.papers_field as A 
JOIN MACG.paper_author as B 
WHERE B.authorID='3206897746' 
AND A.paperID=B.paperID 
AND fieldID IN (159384605, 36464697, 4379982, 83893533);
"""

cursor.execute(select_query)
results = cursor.fetchall()

# Insert the results into the scigene_visualization_field.paper_author_filed table
insert_query = """
INSERT INTO scigene_visualization_field.paper_author_field (paperID, authorID, authorOrder) 
VALUES (%s, %s, %s);
"""

cursor.executemany(insert_query, results)
conn.commit()

# Close the connection
cursor.close()
conn.close()