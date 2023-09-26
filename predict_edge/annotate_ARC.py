# -*- coding: utf-8 -*-
import os
import json
import collections
import sys
import time
from pathlib import Path
from typing import List
import re
import random
from collections import Counter

global fenge
fenge = '=========================================================================='


def main():
    pdf_root_path = str(sys.argv[0])
    pdf_root_path = pdf_root_path.rsplit('/',2)[0]
    annotator = input('Welcome! Annotator: ')
    # # old_feature_path = input("Please provide the path to predicted features:")
    # old_feature_path = input("带有引用类别预测结果的feature文件目录:")
    # # old_json_path = input("Please provide the path to the original json files:")
    # old_json_path = input("20220208最新版json文件目录:")
    # # new_json_path = input("Please provide the path where you want to create the annotate result:")
    # new_json_path = input("标注结果json保存至:")
    # # pdf_path = input("Please provide the path of the pdfs:")
    # pdf_path = input("所有文章的pdf存放目录（将使用默认pdf阅览器）:")
    old_feature_path = '../prediction_result/'
    old_json_path = 'E:/Project_Crimson/acl_json/'
    new_json_path = '../annotated_json/'
    pdf_path = 'E:/Project_Crimson/pdf/'
    if pdf_path.startswith('..'):
        pdf_path = pdf_root_path+pdf_path[2:]
    annotated_extend_list = os.listdir(new_json_path)
    count = 0
    count_all = 0
    for files in annotated_extend_list:
        path = os.path.join(new_json_path,files)
        file = open(path,'r')
        content = json.load(file)
        for items in content['citation_contexts']:
            if 'citation_function' in items.keys() and items['citation_function'] == 'Extends':
                count += 1
        file.close()
    print('\nAlready annotated \033[0;34m'+str(count)+ '\033[0m extend citations.\n')
    time.sleep(4)
    check_path_legitimacy([old_feature_path,old_json_path,pdf_path,new_json_path])
    useful_files_list = extend_files_extract(old_feature_path)
    json_annotation(old_json_path,useful_files_list,new_json_path,pdf_path,annotator)


def extend_files_extract(ftr_result_path:str):
    target_citation_list = collections.defaultdict()
    ftr_file_list = os.listdir(ftr_result_path)
    count = 0
    count_all = 0
    for files in ftr_file_list:
        path = os.path.join(ftr_result_path,files)
        file = open(path,'r')
        line = file.readline()
        target_citation_id_list = []
        while len(line) > 0 :
            line = line.split()
            count_all +=1
            if line[-1] == 'Inherited':
                target_citation_id_list.append(line[2])
                count+=1
            line = file.readline()
        if target_citation_id_list == []:
            continue
        else:
            paper_ID = files.rsplit('.',1)[0]
            target_citation_list[paper_ID] = target_citation_id_list
    print(fenge+'\n\n')
    print('all_citation_count:'+str(count_all))
    print('Find '+str(count)+' extend predictions in '+ str(len(target_citation_list)) + ' papers')
    fenge_1()
    print('\n\nStarting annotation')
    fenge_1()
    return target_citation_list
    # 需要从之前分类得到的.ftr文件中获取被分类为Extend类型的引用
    # 使用引用编号作为身份标识


def json_annotation(file_path:str,useful_citations:dict,output_path:str,pdf_path:str,annotator:str):
    citation_function_list = {'1':'Extends','2':'None','0':'Check pdf','exit':'exit','skip':'skip','add':'addx'}
    if not os.path.isfile('annotation_log.log'):
        fp = open('annotation_log.log','w')
        fp.close()
    log_annotated_files = open('annotation_log.log','r')
    line = log_annotated_files.readline()
    annotated_list = []
    while len(line) > 3:
        annotated_list += [line.strip('\n')]
        line = log_annotated_files.readline()
    log_annotated_files.close()
    log_annotated_files = open('annotation_log.log','a')
    print('loaded annotation log\n')
    list_of_papers = list(useful_citations.keys())
    random.shuffle(list_of_papers)
    for file_IDs in list_of_papers:
        if file_IDs in annotated_list:
            continue
        file_json = open(file_path+'/'+file_IDs+'.json','r')
        file = json.load(file_json)
        if file['year'] not in range(1995,2015):
            continue
        file['annotator'] = annotator
        cited_paper_id_list = []
        annotated_citation_function = ['']*len(file['citation_contexts'])
        print('\n\nNew paper, paperID:    '+file['paper_id'])
        fenge_1()
        skip_flag = 0
        for items in file['citation_contexts']:
            if 'citation_function' in items.keys():
                continue
            if items['citation_id'] not in useful_citations[file_IDs] and random.random() < 0.7:
                continue
            if items['cited_paper_id'].startswith('Ext') and random.random() < 0.7:
                continue
            try:
                if not re.search('prior|previous|continu|exten|optim|follo',items['cite_context'].lower()):
                    if random.random() < 0.2:
                        continue
            except:
                continue
            # 免去标注别的类别的引用,以及对于没有关键词的句子进行筛除
            if items['cited_paper_id'] in cited_paper_id_list:
                count = cited_paper_id_list.count(items['cited_paper_id'])
                temp = -1
                index = []
                for i in range(count):
                    temp = cited_paper_id_list.index(items['cited_paper_id'],temp+1,len(cited_paper_id_list))
                    index.append(temp)
                previous_annotations = [annotated_citation_function[x] for x in index]
                print('\033[0;31mThis paper has been cited earlier in this paper, the prvious annotate result is:\033[0;33m',previous_annotations,'\033[0m')
            # 把在文章中之前出现过的引用标注结果进行展示,如果是空的话说明在训练集中上一个引用没有被使用到
            cited_paper_id_list.append(items['cited_paper_id'])
            cutted_para = cut_the_cite_sentence(items['cite_context'])
            print(fenge)
            print('\033[0;32mCitation ID:  '+items['citation_id']+'\033[0m')
            print(cutted_para[0]+'\033[0;31m'+cutted_para[1]+'\033[0m'+cutted_para[2])
            print(fenge)
            try:
                name = file['sections'][0]['subsections'][0]['sentences'][0]['text']
            except:
                name = "ParsCit has missed this paper's title"
            print('\033[0;32mCiting paper title:\033[0m\n' + name)
            print('\033[0;32mCited paper title:\033[0m\n'+items['raw_string']+'\n'+fenge)
            type = input("\033[0;34mplease enter the annotate result:(\033[0;35m'1' for extends\033[0m,\033[0;32m'2' for others\033[0;33m ,if you want to see the PDF,type '0'\033[0m)\n")
            while type not in citation_function_list.keys():
                type = input("\033[0;34mplease input a valid number:\n\033[0m")
            if citation_function_list[type] == 'None':
                annotated_citation_function[file['citation_contexts'].index(items)] = citation_function_list[type]
                continue
            elif citation_function_list[type] == 'Extends':
                items['citation_function'] = 'Extends'
                annotated_citation_function[file['citation_contexts'].index(items)] = citation_function_list[type]
            elif citation_function_list[type] == 'Check pdf':
                print(fenge+'\n\nAttention : This citation appears in section\033[0;31m '+file['sections'][items['section']]['title']+' \033[0m,sentence '+str(items['sentence']))
                print('..')
                time.sleep(1)
                print('.')
                time.sleep(1)
                print('\nOpen the PDF file for you')
                time.sleep(1)
                os.startfile(pdf_path+'/'+file['paper_id']+'.pdf')
                type = input("\033[0;34mplease enter the annotate result:(\033[0;35m'1' for extends\033[0m,\033[0;32m'2' for others\033[0;33m ,if you want to see the PDF,type '0'\033[0m)\n")
                while type not in citation_function_list.keys():
                    type = input("\033[0;34mplease input a valid number:\n\033[0m")
                if citation_function_list[type] == 'None':
                    annotated_citation_function[file['citation_contexts'].index(items)] = citation_function_list[type]
                    continue
                elif citation_function_list[type] == 'Extends':
                    items['citation_function'] = 'Extends'
                    annotated_citation_function[file['citation_contexts'].index(items)] = citation_function_list[type]
                elif type == 'add':
                    title_part = input('\nplease provide some parts of the cited paper title: if there are no extends left, type in [no] ')
                    while title_part != 'n':
                        for fuzhu_items in file['citation_contexts']:
                            if fuzhu_items['raw_string'].find(title_part) >=0:
                                key_input = input('is it |||' + fuzhu_items['raw_string'] + '?\ny/n? ')
                                if key_input == 'y':
                                    fuzhu_items['citation_function'] = 'Extends'
                                    print('successfully annotated new extend')
                                    break
                                else:
                                    continue
                        title_part = input(
                            '\nany other title you want to search for? if no type [n]: ')
                else:
                    exit(0)
            elif type == 'exit':
                exit(10)
            elif type == 'skip':
                skip_flag == 1
                break
        if skip_flag == 1:
            continue
        extend_list = []
        cited_id_list = []
        for items in file['citation_contexts']:
            cited_id_list.append(items['cited_paper_id'])
            try:
                if items['citation_function'] == 'Extends':
                    extend_list.append(items['cited_paper_id'])
            except:
                continue
        count_dict = Counter(cited_id_list)
        for items in file['citation_contexts']:
            if items['cited_paper_id'] in extend_list:
                items['citation_function'] = 'Extends'
                items['num_occurrence'] = count_dict[items['cited_paper_id']]
        with open(output_path+'/'+file_IDs+'.json','w') as new:
            json.dump(file,new)
        log_annotated_files.write(file_IDs + '\n')
        log_annotated_files.flush()
        file_json.close()
        new.close()
    log_annotated_files.close()


def check_path_legitimacy(path:List[str]) -> bool:
    for paths in path:
        path_n = Path(paths)
        if not path_n.is_dir() or not path_n.exists():
            print('invalid path detected')
            exit(10)


def fenge_1():
    print('\n\n'+fenge)
    time.sleep(2)


def cut_the_cite_sentence(para:str) -> List[str]:
    index = int(len(para)/2)
    left_index = para.rfind('.',0,index-20)
    right_index = para.find('.',index+10)
    return [para[:left_index+1],para[left_index+1:right_index+1],para[right_index+1:]]


#included_id = ['PXX-1','JXX','WXX','DXX','QXX','CXX-1','IXX-1','IXX-2','NXX','OXX']


if __name__ == '__main__':
    main()
