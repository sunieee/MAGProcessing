

# export fieldName=visualization
# export numOfTopAuthors=1100

export database=scigene_visualization_field

# mysql -h192.168.0.140 -uroot -proot -e"drop database scigene_${fieldName}_field_pcg;"
# mysql -h192.168.0.140 -uroot -proot -e"create database scigene_${fieldName}_field_pcg;"
rm -rf data/$database
mkdir -p data/$database/log
mkdir -p data/$database/map
mkdir -p data/$database/papers
mkdir -p data/$database/links

multiprocess() {
    script=$1
    multiprocess_num=20
    # 使用process数量等于CPU核心数能够达到最佳性能

    pids=()

    for ((i=0; i<multiprocess_num; i++)); do
        python ${script}.py $((i+1))/$multiprocess_num > data/$database/log/${script}_$((i+1)).log &
        pids[$i]=$!
        sleep 0.5
    done

    for pid in ${pids[*]}; do
        wait $pid
    done
}

# 第一步需要30min，后面步骤快
multiprocess build_top_author

python create_local_mapping.py > data/$database/log/create_local_mapping.log
multiprocess compute_top_author_key_papers
multiprocess dump_top_author
# python stat_top_author.py $1 200
# 先看非法字符 然后去对应目录输入python data-trans.py