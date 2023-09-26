import os
import random
import sys
import json


def main():
    root_path = str(sys.argv[0])
    new_file = open('../previous_people_dataset.csv','w')
    path = root_path.rsplit('/', 1)[0]+'/annotated-json-data/'
    json_list = os.listdir(path)
    for file in json_list:
        file_path = os.path.join(path,file)
        json_file = json.load(open(file_path,'r'))
        for cites in json_file['citation_contexts']:
            try:
                if cites['citation_function'] == 'Extends':
                    new_file.writelines(','.join([json_file['paper_id'],cites['cited_paper_id'],cites['raw_string'],'\n']))
                    sentences = cut_the_cite_sentence(cites['cite_context'])
                    print(sentences[0]+'\033[0;34m'+sentences[1]+'\033[0m'+sentences[2]+'\n\n')
            except:
                continue

def cut_the_cite_sentence(para:str):
    index = int(len(para)/2)
    left_index = para.rfind('.',0,index-20)
    right_index = para.find('.',index+20)
    return [para[:left_index+1],para[left_index+1:right_index+1],para[right_index+1:]]

if __name__ == '__main__':
    main()