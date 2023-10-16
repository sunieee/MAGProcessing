
# 调整缓冲区的大小，但不完全禁用它，可以使用stdbuf命令。stdbuf命令允许你修改标准输入、标准输出和标准错误的缓冲。
# stdbuf -o0 ./create_field.sh > log/scigene_visualization2_field.log

# export database=scigene_visualization_field
export database=scigene_database_field

# rm -rf out/$database
# mkdir -p out/$database

python extract_scigene_field.py > out/$database/extract_scigene_field.log
python extract_citation_timeseries.py > out/$database/extract_citation_timeseries.log
# python set_fellow.py $2

export filterCondition='PaperCount_field > 20'
python match_author.py > out/$database/match_author.log

# out/{database}/groups.csv 是所有pair
# out/{database}/match.csv 是按照条件筛选后的pair
# out/{database}/filtered.csv 需要**手动**筛选，得到最终需要合并的人

# 筛选规则
# 1. 对于 完全一致(similarity = 1 & lev_dis = 0)的人名，删除所有中文人名，基本上都是重名
# 2. 其他人名手动筛选，看看是不是缩写，或者多了一个名，在 google scholar 上搜索一下，看看是不是同一个人

# 注意：最后一个脚本在手动筛选之后再运行
# python merge_author.py > out/$database/merge.log