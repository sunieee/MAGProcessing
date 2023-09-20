set -e
cd /home/leishi/scigene/dataset/MAG/field_edge
python create_selflink.py scigene_$1_field hI
python similarity_mysql.py scigene_$1_field hI
printf "luoyuankai's password is luoyk2021\n"
ls -l similarity_mysql.csv
printf "scp /home/leishi/scigene/dataset/MAG/field_edge/similarity_mysql.csv luoyuankai@192.168.0.140:~/gensim/\n"
# to 140
# cd /home/luoyuankai/gensim/
# ls -l similarity_mysql.csv
# python Doc2vec_gensim.py && python preprocessing.py &&  ls -l similarity_features.csv && printf "scp similarity_features.csv root@192.168.0.118:/home/leishi/scigene/dataset/MAG/field_edge/\n"
# 118root:vis2021