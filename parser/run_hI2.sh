set -e
ls -l /home/leishi/scigene/dataset/MAG/field_edge/similarity_features.csv
cd /home/leishi/scigene/dataset/MAG/field_edge
sh ./all_features.sh all_dataset_link scigene_$1_field papers_field_citation_timeseries
ls -l ./all_features.csv
cp /home/leishi/scigene/dataset/MAG/field_edge/all_features.csv /home/leishi/scigene/dataset/MAG/parser/data/csv/$1_hI
printf "scp /home/leishi/scigene/dataset/MAG/field_edge/all_features.csv jiyw@192.168.0.140:~/project/GeneticFlow/Award_Inference_Task/GeneticFlow_Graphs/%s_hI\n" $1
