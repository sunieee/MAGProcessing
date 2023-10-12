# to 140
# cd /home/luoyuankai/gensim/
# ls -l similarity_mysql.csv
# python Doc2vec_gensim.py && python preprocessing.py && scp similarity_features.csv root@192.168.0.118:/home/leishi/scigene/dataset/MAG/field_edge/
set -e
#sh ./all_features.sh all_dataset_link scigene_NLP_field papers_field_citation_timeseries

export database=scigene_visualization_field

rm -rf out/$database
mkdir -p out/$database

python compute_similarity_features.py > out/$database/compute_similarity_features.log
python run_extract_features.py > out/$database/run_extract_features.log
python compute_proba.py > out/$database/compute_proba.log

python analysis_distribution.py
# python tosql.py

# ./run.sh > log/$database/run.log 2>&1 &