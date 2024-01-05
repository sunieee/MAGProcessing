export user=root
export password=root
export field=AI

# rm -rf out/$database
mkdir -p out/$field/{csv,log}

python match_conference_journal.py | tee out/$field/log/match_conference_journal.log
python extract_paperID.py | tee out/$field/log/extract_paperID.log
python extract_scigene_field.py | tee out/$field/log/extract_scigene_field.log
python merge_scigene_field.py | tee out/$field/log/merge_scigene_field.log
python extract_abstract.py | tee out/$field/log/extract_abstract.log
python match_author.py | tee out/$field/log/match_author.log
# 注意：最后一个脚本在手动筛选之后放到`out/{field}/match_modify.csv`，再运行
python merge_author.py | tee out/$field/log/merge.log