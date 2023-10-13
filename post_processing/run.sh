
export database=scigene_visualization_field


rm -rf out/${database}
mkdir -p out/${database}/links
# cp -r ../compute_node/out/${database}/links out/${database}/links
cp -r ../compute_node/out/${database}/papers out/${database}/papers
cp ../compute_node/out/${database}/top_field_authors.csv out/${database}

cp ../compute_edge/out/${database}/edge_proba.csv out/${database}
python update_links.py > out/${database}/update_links.log

mkdir -p out/${database}/new_papers
python update_papers.py > out/${database}/update_papers.log
rm -rf out/${database}/papers
mv out/${database}/new_papers out/${database}/papers

# scp out/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
# scp out/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/