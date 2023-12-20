while pgrep -x "python compute_key_papers.py" > /dev/null; do
    sleep 1
done

while pgrep -x "python update_papers.py" > /dev/null; do
    sleep 1
done

export user=root
export password=root
export topN=5000

compute() {
    field=$1
    export field=$field
    # rm -rf out/$field/{papers_raw,papers,links,log}
    mkdir -p out/$field/{papers_raw,papers,links,log}
    
    # # compute node
    python compute_key_papers.py > out/$field/log/compute_key_papers.log
    python update_papers.py > out/$field/log/update_papers.log

    # # compute edge
    python graph.py > out/$field/log/graph.log
    python compute_similarity_features.py > out/$field/log/compute_similarity_features.log
    python run_extract_features.py > out/$field/log/run_extract_features.log
    python compute_link_prob.py > out/$field/log/compute_proba.log
    python update_links.py > out/$field/log/update_links.log

    # draw distribution
    python analyse_distribution.py > out/$field/log/analyse_distribution.log
}


# compute VCG
# compute visualization
# compute CG
# compute AI
compute SE



# scp out/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
# scp out/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/

# rsync -a out/scigene_visualization_field/ root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/scigene_visualization_field/ --progress=info2
# rsync -a out/scigene_field_field/ root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/scigene_field_field/ --progress=info2

# rsync -a output xiaofengli@120.55.163.114:/home/xiaofengli/pyCode/GFVis/csv/ --progress=info2

rsync -a --progress=info2 out/SE/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/SE/
