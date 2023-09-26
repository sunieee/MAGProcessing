# 提取某个领域的论文的venu和所有作者
import mysql.connector
import pandas as pd
import os
import time

mag_conn = mysql.connector.connect(host="192.168.0.140",
                                   user="root",
                                   password="root",
                                   database="MACG")
conn = mysql.connector.connect(host="192.168.0.140",
                               user="root",
                               password="root",
                               database="scigene_visualization_field")
directory = "./unprocessed/"
new_directory = "./processed/"

query_paper = "select ConferenceID, JournalID from papers_field where paperID=%s"
query_conference = "select abbreviation, name from conferences where conferenceID=%s"
query_journal = "select name from journals where journalID=%s"
query_author = "select authors_field.name from paper_author_field join authors_field on paper_author_field.authorID=authors_field.authorID where paper_author_field.paperID=%s order by paper_author_field.authorOrder;"

df_top = pd.DataFrame()
for index in range(1, 995):
    print(index)
    df = pd.read_csv("./sort_csv/papers_" + str(index) + ".csv", sep=',', index_col=0)
    df_top = pd.concat([df_top, df])

mag_cursor = mag_conn.cursor()
cursor = conn.cursor()
files = os.listdir(directory)
for file in files:
    if file.startswith("papers_"):
        print(file)
        papers = pd.read_csv(os.path.join(directory, file), sep=',', index_col=0)
        papers = papers.drop(columns=["authorOrder", "firstAuthorID", "firstAuthorName"])

        venus, author_list, topics = [], [], []

        paperID_list = papers["paperID"].values.tolist()
        for paperID in paperID_list:
            # 处理会议期刊
            cursor.execute(query_paper, (paperID,))
            result = cursor.fetchone()
            if result[0] != '0':
                mag_cursor.execute(query_conference, (result[0],))
                res = mag_cursor.fetchone()
                if res != None:
                    venus.append(res[1] + ' (' + res[0] + ')')
                else:
                    venus.append(None)
            elif result[1] != '0':
                mag_cursor.execute(query_journal, (result[1],))
                res = mag_cursor.fetchone()
                if res != None:
                    venus.append(res[0])
                else:
                    venus.append(None)
            else:
                venus.append(None)
            
            # 处理作者列表
            cursor.execute(query_author, (paperID,))
            result = cursor.fetchall()
            if result:
                names = [name[0] for name in result]
                author_list.append(', '.join(names))
            else:
                author_list.append(None)

            # 处理topic
            topic = df_top[df_top["paperID"] == paperID]
            if topic.empty:
                topics.append(None)
            else:
                topics.append(int(topic["topic"].values.tolist()[0]))

        papers.insert(6, "authorsName", author_list)
        papers.insert(6, "venu", venus)
        papers.insert(9, "topic", topics)
        papers.to_csv(os.path.join(new_directory, file), sep=',')

cursor.close()
mag_cursor.close()
conn.close()
mag_conn.close()