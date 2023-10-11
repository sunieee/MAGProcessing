
export database=scigene_visualization_field

mkdir -p out/$database

python match_author.py

# out/{database}/groups.csv 是所有pair
# out/{database}/match.csv 是按照条件筛选后的pair
# out/{database}/filtered.csv 需要**手动**筛选，得到最终需要合并的人

# 筛选规则
# 1. 对于 完全一致(similarity = 1 & lev_dis = 0)的人名，删除所有中文人名，基本上都是重名
# 2. 其他人名手动筛选，看看是不是缩写，或者多了一个名，在 google scholar 上搜索一下，看看是不是同一个人

python merge_author.py > out/$database/merge.log