# 开头有csv match.csv
rm csv/* && tar -zxvf csv.tar.gz -C csv && mv csv/visualization/* csv && rm -r csv/visualization
python merge_paper.py
python merge_author.py
# 抽取abstract，目前暂时从118传过来
python process_paper.py
# extends_prob暂时为1，citationcontext暂时从118获取 注意extends_prob要改为extendsProb
cp ~/download/link_0.csv processed/links_3206897746.csv
python add_author.py
cp -f sort_csv/* online/
cp -f final/* online/
cp -f add_top_field_authors.csv online/top_field_authors.csv