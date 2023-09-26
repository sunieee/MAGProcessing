import  json

file = open('../new_txt.txt','r')
line = file.readline()
json_path = 'E:/Project_Crimson/acl_json/'
new_file = open('../special_num_occ.txt','w')
while len(line)> 3:
    line = line.split('\t')
    file_name = line[0]+'.json'
    try:
        file_ano = open(json_path+file_name,'r')
    except:
        line = file.readline()
        continue
    json_file = json.load(file_ano)
    num_occur = 0
    for item in json_file['citation_contexts']:
        if item['cited_paper_id']  == line[1]:
            num_occur += 1
    file_ano.close()
    new_file.writelines('+'.join([line[0],line[1],str(num_occur)])+'\n')
    line = file.readline()
new_file.close()