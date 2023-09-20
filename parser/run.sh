mysql -uroot -pVis_2014 -e"drop database scigene_$1_field_pcg;"
mysql -uroot -pVis_2014 -e"create database scigene_$1_field_pcg;"
rm -f /home/leishi/scigene/dataset/MAG/parser/data/csv/$1/*.csv
mkdir /home/leishi/scigene/dataset/MAG/parser/data/csv/$1
chmod 777 /home/leishi/scigene/dataset/MAG/parser/data/csv/$1
python build_top_author_MAG_graphs.py $1 200
python compute_top_author_key_papers_MAG_graphs.py $1 200
# python dump_top_author_MAG_graphs.py $1 200
# python stat_top_author_MAG_graphs.py $1 200
# 先看非法字符 然后去对应目录输入python data-trans.py
# cd field
# sh compute_field.sh