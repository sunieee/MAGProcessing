# python create_selflink.py scigene_NLP_field 
# python similarity_mysql.py scigene_NLP_field 
# scp similarity_mysql.csv luoyuankai@192.168.0.140:~/gensim/
# luoyuankai's password is luoyk2021
# to 140
# cd /home/luoyuankai/gensim/
# ls -l similarity_mysql.csv
# python Doc2vec_gensim.py && python preprocessing.py && scp similarity_features.csv root@192.168.0.118:/home/leishi/scigene/dataset/MAG/field_edge/
# 118root:vis2021
set -e
#sh ./all_features.sh all_dataset_link scigene_NLP_field papers_field_citation_timeseries
python compute_similarity_features.py
python run_extract_features.py
python compute_proba.py
# python tosql.py