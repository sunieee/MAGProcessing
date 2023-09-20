###database HCI NLP ComputerSecurity DataMining SoftwareEngineering ComputerGraphicsImages
# mysql -uroot -pVis_2014 -e"drop database scigene_$1_field_hIpcg;"
# mysql -uroot -pVis_2014 -e"create database scigene_$1_field_hIpcg;"
# rm -f /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI/*.csv
# mkdir /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# chmod 777 /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# set -e
# python build_tophI_author_MAG_graphs.py $1 500
# mysql -uroot -pVis_2014 -e"update scigene_$1_field.authors_field_hI set authorRank=NULL where hIndex_field is NULL"
# python compute_tophI_author_key_papers_MAG_graphs.py $1 500
# python dump_tophI_author_MAG_graphs.py $1 500
# cp /home/leishi/scigene/dataset/MAG/parser/data/csv/HCI/data-trans.py /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
# chmod -R 777 /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
#去看对应目录下有无非法字符，比如\"
set -e
cd /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
python data-trans.py
cd /home/leishi/scigene/dataset/MAG/parser/field
python compute_matrix.py $1_hI
python compute_field.py $1_hI
# cd /home/leishi/scigene/dataset/MAG/parser/data/csv
# zip -q -r $1_hI.zip $1_hI
# printf  "scp /home/leishi/scigene/dataset/MAG/parser/data/csv/%s_hI.zip jiyw@192.168.0.140:~/project/GeneticFlow/Award_Inference_Task/GeneticFlow_Graphs\n" $1