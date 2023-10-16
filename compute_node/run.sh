

# export fieldName=visualization
# export numOfTopAuthors=1100

# export database=scigene_visualization_field
export database=scigene_database_field
export filterCondition='PaperCount_field > 20'

# mysql -h192.168.0.140 -uroot -proot -e"drop database scigene_${fieldName}_field_pcg;"
# mysql -h192.168.0.140 -uroot -proot -e"create database scigene_${fieldName}_field_pcg;"
rm -rf out/$database
mkdir -p out/$database/map
mkdir -p out/$database/papers

# 第一步需要10min，后面步骤快
python create_local_mapping.py > out/$database/create_local_mapping.log
python compute_top_author_key_papers.py > out/$database/compute_top_author_key_papers.log

python analyse_distribution.py
# python stat_top_author.py $1 200
# 先看非法字符 然后去对应目录输入python data-trans.py

# python create_mapping.py > out/$database/create_mapping.log