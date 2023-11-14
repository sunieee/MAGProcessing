export user=root
export password=root
# export database=scigene_visualization_field
# export database=scigene_database_field
# export database=scigene_VCG_field
export database=scigene_CG_field

rm -rf out/${database}
mkdir -p out/${database}/links
mkdir -p out/${database}/papers

python update_links.py > out/${database}/update_links.log
python update_papers.py > out/${database}/update_papers.log



'''
scp out/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
scp out/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/

rsync -a out/scigene_visualization_field/ root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/scigene_visualization_field/ --progress=info2
rsync -a out/scigene_database_field/ root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/scigene_database_field/ --progress=info2
'''