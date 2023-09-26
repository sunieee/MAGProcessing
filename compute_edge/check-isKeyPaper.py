import pandas as pd
# papers_arc_christophermanning8
# papers_arc_pushpakbhattacharyya176
df_hIndex = pd.read_csv("../acl_hI/papers_arc_zhiyuanliu152.csv", sep=',', header=None)
df_paperCount = pd.read_csv("./papers_arc_zhiyuanliu93.csv", sep=',', header=None)
abs_diff_sum = abs(df_paperCount[6] - df_hIndex[6]).sum()
print(abs_diff_sum)