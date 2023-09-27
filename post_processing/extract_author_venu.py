# 提取某个领域的论文的venu和所有作者
import mysql.connector
import pandas as pd
import time

conn = mysql.connector.connect(host="192.168.0.140",
                               user="root",
                               password="root",
                               database="MACG")
cursor = conn.cursor()
conn_field = mysql.connector.connect(host="192.168.0.140",
                               user="root",
                               password="root",
                               database="scigene_acl_anthology")  # TODO
cursor_field = conn_field.cursor()

query = "select ConferenceID, JournalID from papers_field where paperID=%s"
query_conference = "select abbreviation, name from conferences where conferenceID=%s"
query_journal = "select name from journals where journalID=%s"
query_author_id = "select authorID from paper_author_ARC where paperID=%s order by authorOrder"
query_author_name = "select * from authors_ARC where authorID=%s"
for i in range(1, 1001):
    filename = "output/papers_" + str(i) + ".csv"
    papers = pd.read_csv(filename, sep=',', index_col=0)
    print(filename)

    paperIDList = papers["paperID"].values.tolist()
    authorList = []
    for paperID in paperIDList:
        cursor_field.execute(query_author_id, (paperID,))
        result = cursor_field.fetchall()
        authors = ""
        for authorID in result:
            cursor_field.execute(query_author_name, (authorID[0],))
            res = cursor_field.fetchone()
            authors += res[1] + ', '
        if (authors != ""):
            authorList.append(authors[:-2])
        else:
            authorList.append(None)
        
        venus = []
        cursor_field.execute(query, (paperID,))
        result = cursor_field.fetchone()
        if result[0] != '0':
            cursor.execute(query_conference, (result[0],))
            res = cursor.fetchone()
            if res != None:
                venus.append(res[1] + ' (' + res[0] + ')')
            else:
                venus.append(None)
        elif result[1] != '0':
            cursor.execute(query_journal, (result[1],))
            res = cursor.fetchone()
            if res != None:
                venus.append(res[0])
            else:
                venus.append(None)
        else:
            venus.append(None)

    papers.insert(7, "authorsName", authorList)
    papers.insert(7, "venu", venus)
    papers.to_csv("csv/papers_" + str(i) + ".csv")

cursor_field.close()
conn_field.close()
cursor.close()
conn.close()