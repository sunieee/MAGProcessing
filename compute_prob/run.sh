# to 140
# cd /home/luoyuankai/gensim/
# ls -l similarity_mysql.csv
# python Doc2vec_gensim.py && python preprocessing.py && scp similarity_features.csv root@192.168.0.118:/home/leishi/scigene/dataset/MAG/field_edge/
set -e
#sh ./all_features.sh all_dataset_link scigene_NLP_field papers_field_citation_timeseries

# export database=scigene_visualization_field
export database=scigene_database_field
export filterCondition='PaperCount_field > 20'

rm -rf out/$database
mkdir -p out/$database/map
mkdir -p out/$database/papers


# compute node
python create_mapping.py > out/$database/create_mapping.log
# python create_local_mapping.py > out/$database/create_local_mapping.log
python compute_key_papers.py > out/$database/compute_top_author_key_papers.log


# compute edge
python compute_similarity_features.py > out/$database/compute_similarity_features.log
python run_extract_features.py > out/$database/run_extract_features.log
python compute_link_prob.py > out/$database/compute_proba.log

python analyse_distribution.py > out/$database/analyse_distribution.log
# python tosql.py

# ./run.sh > log/$database/run.log 2>&1 &