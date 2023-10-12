
# 调整缓冲区的大小，但不完全禁用它，可以使用stdbuf命令。stdbuf命令允许你修改标准输入、标准输出和标准错误的缓冲。
# stdbuf -o0 ./create_field.sh > log/scigene_visualization2_field.log

export database=scigene_visualization_field

rm -rf out/$database
mkdir -p out/$database

python extract_scigene_field.py > out/$database/extract_scigene_field.log
python extract_citation_timeseries.py > out/$database/extract_citation_timeseries.log
# python set_fellow.py $2