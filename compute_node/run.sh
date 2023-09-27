

export fieldName=visualization
export numOfTopAuthors=1000

# mysql -h192.168.0.140 -uroot -proot -e"drop database scigene_${fieldName}_field_pcg;"
# mysql -h192.168.0.140 -uroot -proot -e"create database scigene_${fieldName}_field_pcg;"
rm -f data/${fieldName}
mkdir data/${fieldName}
python build_top_author_MAG_graphs.py
python compute_top_author_key_papers_MAG_graphs.py
python dump_top_author_MAG_graphs.py
# python stat_top_author_MAG_graphs.py $1 200
# 先看非法字符 然后去对应目录输入python data-trans.py
# cd field
# sh compute_field.sh