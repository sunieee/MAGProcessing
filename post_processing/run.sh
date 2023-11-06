
# export database=scigene_visualization_field
export database=scigene_database_field
# export database=scigene_VCG_field


rm -rf out/${database}
mkdir -p out/${database}/links
mkdir -p out/${database}/papers

# cp -r ../compute_node/out/${database}/links out/${database}/links
cp -r ../compute_prob/out/${database}/papers out/${database}/papers_unprocessed
cp ../compute_prob/out/${database}/top_field_authors.csv out/${database}
cp ../compute_prob/out/${database}/edge_proba.csv out/${database}
cp ../compute_prob/out/${database}/paperID2abstract.json out/${database}

python update_links.py > out/${database}/update_links.log
python update_papers.py > out/${database}/update_papers.log

# scp out/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
# scp out/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/