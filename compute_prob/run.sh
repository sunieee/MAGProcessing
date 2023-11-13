set -e

# export database=scigene_visualization_field
# export database=scigene_database_field
# export database=scigene_VCG_field
export database=scigene_CG_field
export topN=5000

# rm -rf out/$database
mkdir -p out/$database/papers

# compute node
python compute_key_papers.py > out/$database/compute_key_papers.log

# compute edge
python compute_similarity_features.py > out/$database/compute_similarity_features.log
python run_extract_features.py > out/$database/run_extract_features.log
python compute_link_prob.py > out/$database/compute_proba.log

python analyse_distribution.py > out/$database/analyse_distribution.log