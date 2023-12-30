export user=root
export password=root
export field=ACMfellowTuring
mkdir -p out/$field/{papers_raw,papers,links,log}

python extract_scholar.py

python extract_abstract.py | tee out/$field/log/extract_abstract.log
python compute_key_papers_scholar.py | tee out/$field/log/compute_key_papers.log
python update_papers.py | tee out/$field/log/update_papers.log

python graph.py | tee out/$field/log/graph.log
python compute_similarity_features.py | tee out/$field/log/compute_similarity_features.log
python extract_link_features.py | tee out/$field/log/extract_link_features.log
python compute_link_prob.py | tee out/$field/log/compute_proba.log
python update_links.py | tee out/$field/log/update_links.log

python analyse_distribution.py | tee out/$field/log/analyse_distribution.log