
export user=root
export password=root
export field=AI

mkdir -p out/$field/{papers_raw,papers,links}
split_count=6

# for i in {0..(split_count-1)} python compute_key_papers.py $i/$split_count | tee out/$field/log/compute_key_papers_$i.log &

for i in $(seq 0 $((split_count-1))); do
    echo "Iteration $i"
    python compute_key_papers.py $i/$split_count | tee out/$field/log/compute_key_papers_$i.log &
    sleep 0.5
done
