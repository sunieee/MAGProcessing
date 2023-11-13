# 生成某一领域topic分布和paper-topic对应关系的脚本
python save_version.py
fieldType="visualization"   # TODO 根据不同的field改动
cp /home/sy/arc/post_processing/out/scigene_${fieldType}_field/papers/* input/${fieldType}/
python bertopic_title_abstract.py ${fieldType} 1 1> output/${fieldType}/bertopic.txt
python words_merge.py ${fieldType}
cp output/${fieldType}/topic_word_prob_merged.json output/${fieldType}/topic_word_prob_manual.json
# 手动更改未合并的词（可选择不做）
# topology emotion scatterplot hmd projection learner ontology organize distortion aoi color colormap treemap hierarchy saliency collaborative dimensional holography molecular genome volume bundling scientific streamline uncertainty
python words_filter.py ${fieldType}
cp output/${fieldType}/topic_location_*.csv output/${fieldType}/topic_location.csv
python concat_topic.py ${fieldType}
python color_topic.py ${fieldType}
# TODO 下面的11需要看topic_distribution.html的分布图确定聚类数目
python cluster_topic.py ${fieldType} 11
python group_to_root.py ${fieldType} 11 1> output/${fieldType}/topic_group.txt

# cd ..
# bash collect_field_file.sh