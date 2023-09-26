# from nltk.corpus import wordnet as wn
import nlpaug.augmenter.word as naw
import nltk
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')

aug = naw.SynonymAug()

file = open('../sentences_and_labels.csv','r',encoding='utf-8')
new_file = open('../augmented_sentences.tsv','w',encoding='utf-8')
line = file.readline()

while len(line) > 3:
    new_file.writelines(line)
    line = line.strip('\n').split('\t')
    label = line[0]
    line = line[1]
    # for words in line:
    #     if len(words)>=5:
    #         synonum_set = set()
    #         synonums = wn.synsets(words)
    #         for syn in synonums:
    #             for syn_word in syn.lemma_names:
    #                 synonum_set.add(syn_word)
    #         if len(synonum_set) > 0:
    augmented_data = aug.augment(line)
    new_file.writelines(label + '\t' + augmented_data + '\n')
    # for data in augmented_data:
    #     new_file.writelines(label+'\t'+data + '\n')
    line = file.readline()
file.close()
new_file.close()

