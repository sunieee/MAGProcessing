while pgrep -x "python compute_key_papers.py" > /dev/null; do
    sleep 1
done

while pgrep -x "python update_papers.py" > /dev/null; do
    sleep 1
done

export user=root
export password=root
export topN=5000

run() {
    database=$1
    export database=$database
    # rm -rf out/$database/{papers_raw,papers,links,log}
    # mkdir -p out/$database/{papers_raw,papers,links,log}

    # # compute node
    python compute_key_papers.py > out/$database/log/compute_key_papers.log
    python update_papers.py > out/$database/log/update_papers.log

    # # compute edge
    python compute_similarity_features.py > out/$database/log/compute_similarity_features.log
    python run_extract_features.py > out/$database/log/run_extract_features.log
    python compute_link_prob.py > out/$database/log/compute_proba.log
    python update_links.py > out/$database/log/update_links.log

    # draw distribution
    python analyse_distribution.py > out/$database/log/analyse_distribution.log
}


run scigene_VCG_field
run scigene_visualization_field
run scigene_database_field
run scigene_CG_field
run scigene_acl_anthology



# scp out/visualization/links_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/
# scp out/visualization/papers_*.csv root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/visualization/

# rsync -a out/scigene_visualization_field/ root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/scigene_visualization_field/ --progress=info2
# rsync -a out/scigene_database_field/ root@82.156.152.182:/home/xfl/pyCode/GFVis/csv/scigene_database_field/ --progress=info2

# rsync -a output xiaofengli@120.55.163.114:/home/xiaofengli/pyCode/GFVis/csv/ --progress=info2