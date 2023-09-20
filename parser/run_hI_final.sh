# mysql -uroot -pVis_2014 -e"drop database scigene_$1_field_hIpcg;"
# mysql -uroot -pVis_2014 -e"create database scigene_$1_field_hIpcg;"
# rm -f /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI/*.csv
# mkdir /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# chmod 777 /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# python build_tophI_author_MAG_graphs.py $1 500
# python compute_tophI_author_key_papers_MAG_graphs.py $1 500
# python dump_tophI_author_MAG_graphs.py $1 500
# cp /home/leishi/scigene/dataset/MAG/parser/data/csv/$1/data-trans.py /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# chmod -R 777 /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# cd /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# #去看对应目录下有无非法字符，比如\"
# python data-trans.py
# cd /home/leishi/scigene/dataset/MAG/parser/field
# python compute_matrix.py $1_hI
# python compute_field.py $1_hI