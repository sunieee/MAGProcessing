
export database=scigene_visualization_field


rm -rf data/${database}
mkdir -p data/${database}/links
# cp -r ../compute_node/data/${database}/links data/${database}/links
cp -r ../compute_node/data/${database}/papers data/${database}/papers
cp ../compute_node/data/${database}/top_field_authors.csv data/${database}

cp ../compute_edge/out/${database}/edge_proba.csv data/${database}
python update_links.py > data/${database}/update_links.log

mkdir -p data/${database}/new_papers
python update_papers.py > data/${database}/update_papers.log
rm -rf data/${database}/papers
mv data/${database}/new_papers data/${database}/papers

# scp data/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
# scp data/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/