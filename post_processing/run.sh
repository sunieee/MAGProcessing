
export fieldName=visualization
rm -rf data/${fieldName}
cp -r ../compute_node/data/${fieldName} data/${fieldName}
cp ../compute_edge/out/edge_proba.csv data/${fieldName}/edge_proba.csv
python update_links.py
python update_papers.py

# scp data/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
# scp data/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/