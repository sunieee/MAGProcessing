import pandas as pd
import os

def find_bounds(lst, a):
    if a < lst[0]:
        return 0
    if a >= lst[-1]:
        return len(lst)
    for i in range(len(lst) - 1):
        if lst[i] <= a < lst[i + 1]:
            return i + 1
err_rank = [283, 762, 880, 882, 966, 969]
# df = pd.read_csv("./data/csv/visualization/top_field_authors.csv", sep=',', names=["authorID", "rank", "name", "PaperCount", "CitationCount", "PaperCount_field", "authorRank", "CitationCount_field", "hIndex_field", "FellowType"])
# # print(df)
# authors = df.values.tolist()
# for author in authors:
#     cnt = find_bounds(err_rank, int(author[6]))
#     author[6] -= cnt
# for i in range(len(err_rank), 0, -1):
#     del authors[err_rank[i - 1]]
# df = pd.DataFrame(authors)
# df.to_csv("../top_field_authors.csv", sep=',', index=False, header=False)

files = os.listdir("./data/csv/visualization/")
for file in files:
    if not os.path.isdir(file):
        if file[-8] == '1':
            authorRank = file[-8:-4]
            tmp = file[:-8]
            tmp += str(int(authorRank) - 6) + ".csv"
            os.system("mv ./data/csv/visualization/" + file + " ./tmp/" + tmp)
        elif file[-7] >= '1' and file[-7] <= '9':
            authorRank = file[-7:-4]
            tmp = file[:-7]
            cnt = find_bounds(err_rank, int(authorRank))
            tmp += str(int(authorRank) - cnt) + ".csv"
            os.system("mv ./data/csv/visualization/" + file + " ./tmp/" + tmp)
            