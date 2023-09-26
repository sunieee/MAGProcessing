import pandas as pd
import os
import re
import mysql.connector

mag_conn = mysql.connector.connect(host="192.168.0.140",
                                   user="root",
                                   password="root",
                                   database="MACG")
directory = "./processed/"
final_directory = "./final/"
sql = "select * from authors where authorID=%s;"

df_top = pd.read_csv("sort_top_field_authors.csv", sep=',',
                     names=["authorID", "rank", "name", "PaperCount", "CitationCount", "PaperCount_field", "authorRank", "CitationCount_field", "hIndex_field", "FellowType"])
print(df_top)

mag_cursor = mag_conn.cursor()
files = os.listdir(directory)
for file in files:
    if file.startswith("papers_"):
        df = pd.read_csv(os.path.join(directory, file), sep=',', index_col=0)
        authorID = re.search(r'\d+', file).group()
        mag_cursor.execute(sql, (authorID,))
        result = mag_cursor.fetchone()
        rank = result[1]
        name = result[2]
        PaperCount = result[3]
        CitationCount = result[4]
        PaperCount_field = len(df)
        CitationCount_field = df["citationCount"].sum()
        hIndex_field = 8
        FellowType = "\\N"
        
        index = df_top[df_top["PaperCount_field"] == PaperCount_field].index[0]
        print(index)
        author = [authorID, rank, name, PaperCount, CitationCount, PaperCount_field, index + 1, CitationCount_field, hIndex_field, FellowType]
        print(author)
        df_authors = pd.concat([df_top.iloc[:index], pd.DataFrame([author], columns=["authorID", "rank", "name", "PaperCount", "CitationCount", "PaperCount_field", "authorRank", "CitationCount_field", "hIndex_field", "FellowType"]), df_top.iloc[index:]]).reset_index(drop=True)
        print(df_authors)
        df_authors["authorRank"] = df_authors["authorRank"].apply(lambda x: x + 1 if x > index else x)    # TODO 这里插入的index必会发生错误，还要重新修改，应该为df[index] = index-1，这次就先不改了
        df_authors.to_csv("add_top_field_authors.csv", sep=',', index=False, header=False)

        filenames = os.listdir("./sort_csv/")
        for filename in filenames:
            if filename.startswith("papers_"):
                id = re.search(r'\d+', filename).group()
                if int(id) > index:
                    link = "links_" + id + ".csv"
                    new_paper = "papers_" + str(int(id) + 1) + ".csv"
                    new_link = "links_" + str(int(id) + 1) + ".csv"
                    os.rename(os.path.join("./sort_csv/", filename), os.path.join(final_directory, new_paper))
                    os.rename(os.path.join("./sort_csv/", link), os.path.join(final_directory, new_link))
        print(file)
        os.rename(os.path.join(directory, file), os.path.join(final_directory, "papers_" + str(index + 1) + ".csv"))
        os.rename(os.path.join(directory, "links_" + str(authorID) + ".csv"), os.path.join(final_directory, "links_" + str(index + 1) + ".csv"))
