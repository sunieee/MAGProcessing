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
python run_extract_cross_correlation_feature.py $1 $2 $3
python run_extract_negativetimelag_cross_correlation_feature.py $1 $2 $3
python run_extract_timelag_cross_correlation_feature.py $1 $2 $3
python run_extract_window_cross_correlation_feature.py $1 $2 $3
python run_extract_window_negativetimelag_cross_correlation_feature.py $1 $2 $3
python run_extract_window_timelag_cross_correlation_feature.py $1 $2 $3
python other_features.py $2
python calculate.py
python run_extract_citation_network_feature.py $1 $2 paper_reference_field
python tosql.py $2