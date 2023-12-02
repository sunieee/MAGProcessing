export user=root
export password=root
# export database=scigene_visualization_field
# export database=scigene_database_field
# export database=scigene_VCG_field
# export database=scigene_CG_field
export database=scigene_AI_field

# rm -rf out/$database
mkdir -p out/$database

# python extract_scigene_field.py > out/$database/extract_scigene_field.log
python extract_citation_timeseries.py > out/$database/extract_citation_timeseries.log
python renew_database.py > out/$database/renew_database.log
# python set_fellow.py $2

python match_author.py > out/$database/match_author.log
# 注意：最后一个脚本在手动筛选之后放到`out/{database}/filtered.csv`，再运行
# python merge_author.py > out/$database/merge.log