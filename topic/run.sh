# 生成某一领域topic分布和paper-topic对应关系的脚本
python bertopic_from_abstract.py 1/2 1> output/bertopic.txt
python merge_words.py
cp output/topic_word_prob_merged.json output/topic_word_prob_manual.json
# 手动更改未合并的词
# topology emotion scatterplot hmd projection learner ontology organize distortion aoi color colormap treemap hierarchy saliency collaborative dimensional holography molecular genome volume bundling scientific streamline uncertainty
python filterTopic.py
python renameTopic.py
python colorTopic.py
python clusterTopic.py 19
python group_to_root.py 19 1> topic_group.txt