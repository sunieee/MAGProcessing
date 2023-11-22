#!/bin/bash
rm -rf output
mkdir output

# 遍历out/目录下以scigene或者visitor开头的文件夹
for folder_a in out/{scigene*,visitor*}; do
    # 检查文件夹是否存在
    if [ -d "$folder_a" ]; then
        # 提取文件夹名
        folder_name=$(basename "$folder_a")
        
        # 在当前目录下创建同名文件夹(b)
        folder_b="output/$folder_name"
        mkdir -p "$folder_b"

        # 移动links, papers文件夹和top_field_authors.csv到b文件夹下
        cp -r "$folder_a/links" "$folder_a/papers" "$folder_a/top_field_authors.csv" "$folder_b/"
        
        echo "复制文件夹 $folder_a 到 $folder_b"
    fi
done