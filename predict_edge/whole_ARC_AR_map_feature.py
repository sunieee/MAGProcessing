import os
import re

def total_citation(list_of_citations:list,paper_json:dict):   #返回多次的均值
    results_1 = [-1]
    results_2 = [-1]
    results_3 = [-1]
    for index in list_of_citations:
        count_section = -1
        count_subsection = -1
        count_sentence = -1
        for citation in paper_json['citation_contexts']:
            if citation['section'] == paper_json['citation_contexts'][index]['section']:
                count_section += 1
                if citation['subsection'] == paper_json['citation_contexts'][index]['subsection']:
                    count_subsection += 1
                    if citation['sentence'] == paper_json['citation_contexts'][index]['sentence']:
                        count_sentence += 1
        results_1.append(count_section)
        results_2.append(count_subsection)
        results_3.append(count_sentence)
    return [sum(results_1)/len(results_1),sum(results_2)/len(results_2),sum(results_3)/len(results_3)]

def relative_positions(list_of_citations:list,paper_json:dict):  #返回多次的最大值最小值均值
    relative_position_sentence = []
    relative_position_subsection = []
    relative_position_section = []
    for index in list_of_citations:
        section_id = paper_json['citation_contexts'][index]['section']
        subsection_id = paper_json['citation_contexts'][index]['subsection']
        sentence_id = paper_json['citation_contexts'][index]['sentence']
        citing_string = paper_json['citation_contexts'][index]['citing_string']
        if section_id == -1:
            relative_position_sentence.append(-1)
            relative_position_subsection.append(-1)
            relative_position_section.append(-1)
        else:
            sentence = paper_json['sections'][section_id]['subsections'][subsection_id]['sentences'][sentence_id]['text']
            relative_position_sentence.append(sentence.index(citing_string) / len(sentence)+1)
            relative_position_subsection.append(sentence_id / len(paper_json['sections'][section_id]['subsections'][subsection_id]['sentences']))
            relative_position_section.append(section_id / len(paper_json['sections']))
    ii = 0
    while len(relative_position_sentence) > 1:
        if ii == len(relative_position_sentence):
            break
        else:
            if relative_position_sentence[ii] == -1:
                relative_position_sentence.pop(ii)
                relative_position_subsection.pop(ii)
                relative_position_section.pop(ii)
            else:
                ii += 1
    result_1 = [min(relative_position_section),max(relative_position_section)]
    result_2 = [min(relative_position_subsection),max(relative_position_subsection)]
    result_3 = [min(relative_position_sentence),max(relative_position_sentence)]
    return result_1+result_2+result_3

def cited_in_where(list_of_citations:list,paper_json:dict):
    result = [0 for jjj in range(9)]   # Introduction(Background)\Related Word\Motivation\Method(Approach)\Evaluation(Experiment)\Discussion(analysis)\Conclusion\Ack
    for index in list_of_citations:
        section_id = paper_json['citation_contexts'][index]['section']
        section_title = paper_json['sections'][section_id]['title']
        if section_title.find('bstract') >= 0:
            result[0] += 1
            continue
        if section_title.find('ntrodu') >= 0 or section_title.find('ackgrou') >= 0:
            result[1] += 1
            continue
        if section_title.find('elated') >= 0:
            result[2] += 1
            continue
        if section_title.find('otivation')>= 0:
            result[3] += 1
            continue
        if section_title.find('ethod')>=0 or section_title.find('pproa')>=0 or section_title.find('odel')>=0 or section_title.find('lgorit')>=0:
            result[4] += 1
            continue
        if section_title.find('xperiment')>=0 or section_title.find('esult')>=0:
            result[5] += 1
            continue
        if section_title.find('valuat')>=0 or section_title.find('iscussi')>=0:
            result[6] += 1
            continue
        if section_title.find('onclusion')>=0:
            result[7] += 1
            continue
        if section_title.find('ckow')>=0:
            result[8] += 1
    return result

def content_key_phrases(list_of_citations:list,paper_json:dict):   #关键句式
    key_phrases = [r'extension',
                   r'we.*extend',
                   r'',
                   r'extend',
                   r'base.*on',
                   r'update',
                   r'previous',
                   r'our.*work'
                   ]
    # key_phrases = ['ed',
    #                'ing',
    #                'work',
    #                'xtend',
    #                ' on',
    #                'reviou',
    #                'ase',
    #                'our'
    #                ] 0.8814409780979549 [0.6212534059945504, 0.393, 0.7169811320754716, 0.5480769230769231]
#phrase_1 : 'n extension',
#phrase_2 : 'e extension',
#phrase_3 : 'continuous',
#phrase_4 : 'e extend',
#phrase_5 : 'further extend',
#phrase_6 : 'which extend',
#phrase_7 : 'our previous',
#phrase_8 : 'enhancement'
    result = [0 for jjj in range(len(key_phrases))]
    for index, citation in enumerate(paper_json['citation_contexts']):
        if index in list_of_citations and citation['cite_context']!= -1:
            sentences = paper_json['sections'][citation['section']]['subsections'][citation['subsection']]['sentences'][
                citation['sentence']]['text']
            for i,keys in enumerate(key_phrases):
                result[i] += len(re.findall(keys),sentences)
        # for i in range(len(result)):
        #     result[i] = result[i] / len(list_of_citations)
    return result

import difflib
import json
file = open('../all_dataset_link_label_with_features.txt','r')
ID_transform_file = open('../MAG_ID_TO_EXTERNAL_AND_TITLE.csv','r')
new_data_file = open('../all_dataset_combined_context_features.txt','w')
# new_sentence_file = open('../sentences_and_labels.csv','w',encoding='utf-8')
feature_name = []
line = file.readline()
ID_transfer_line = ID_transform_file.readline()
title_dict = dict()
ID_dict = dict()
count = 0
while len(ID_transfer_line) > 3:
    ID_transfer_line = ID_transfer_line.strip('\n').split(',')
    title_dict[(ID_transfer_line[2].strip())] = ID_transfer_line[1].strip()
    ID_dict[ID_transfer_line[2]] = ID_transfer_line[0].strip('\n').strip('\r').strip()
    ID_transfer_line = ID_transform_file.readline()
while len(line) > 3:
    line = line.split('+')
    citingpaperID = line[0]
    citedpaperID = line[1]
    flag = 0
    line[-1] = line[-1].strip('\r').strip()
    try:
        origin_json = open('E:/Project_Crimson/acl_json/'+citingpaperID+'.json','r')
    except:
        print('missing ID: '+ citingpaperID)
        line = file.readline()
        continue
    json_file = json.load(origin_json)
    if citedpaperID.isdigit():
        try:
            if ID_dict[citedpaperID].find('DUMMY'):
                citedpaperID = title_dict[citedpaperID]
                flag = 1
            else:
                citedpaperID = ID_dict[citedpaperID]
        except:
            line = file.readline()
            continue
    list_of_citations = []
    if flag == 1:
        ratio_max = 0
        for index,context in enumerate(json_file['citation_contexts']):
            ratio_matched = difflib.SequenceMatcher(None,context['info']['title'],citedpaperID).ratio()
            if ratio_matched > ratio_max:
                ratio_max = ratio_matched
                list_of_citations = [index]
            elif ratio_matched == ratio_max:
                list_of_citations.append(index)
    else:
        for index,context in enumerate(json_file['citation_contexts']):
            if context['cited_paper_id'] == citedpaperID:
                list_of_citations.append(index)
    if list_of_citations == []:
        line.extend([-2 for _ in range(26)])
    else:
        # for ids in list_of_citations:
        #     section_id = json_file['citation_contexts'][ids]['section']
        #     subsection_id = json_file['citation_contexts'][ids]['subsection']
        #     sentence_id = json_file['citation_contexts'][ids]['sentence']
        #     if section_id == -1:
        #         continue
        #     sentence = json_file['sections'][section_id]['subsections'][subsection_id]['sentences'][sentence_id]['text']
        #     sentence = sentence.replace('+','')
        #     label = line[2]
        #     new_sentence_file.writelines('\t'.join([label,sentence])+'\n')
        line.extend(total_citation(list_of_citations,json_file))
        line.extend(relative_positions(list_of_citations,json_file))
        line.extend(cited_in_where(list_of_citations,json_file))
        line.extend(content_key_phrases(list_of_citations,json_file))
    line = [str(x) for x in line]
    # line = [line[x] for x in [0, 1, 2, 5, 8, 9, 10, 11, 12, 13, 16, 19] + [i for i in range(20,len(line))]]
    for i in range(len(line)):
        if line[i] == '-2':
            line[i] == '\\N'
    new_data_file.writelines('+'.join(line)+'\n')
    line = file.readline()
    count += 1
    if count % 100 == 0:
        print(count)
file.close()
ID_transform_file.close()
new_data_file.close()
# new_sentence_file.close()