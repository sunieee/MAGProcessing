import re
file1 = open(r'C:\Users\28103\Desktop\citation-function-master\resources\paper_list.txt','r')
file2 = open(r'C:\Users\28103\Desktop\citation-function-master\resources\arc-paper-ids_1.tsv','a',newline = '')

data = ['','','','','']
line = file1.readline()
line = line.strip()
line = line.split('\t')
data = line

while len(line) >0 and len(line[0])>0:
    if  len(line) < 4:
        line = file1.readline()
        line = line.strip()
        line = line.split('\t')
        data = line
        print('skip for loss')
        print(line)
        continue
    if int(data[1]) < 2015 and not data[0][0].isdigit():
        line = file1.readline()
        line = line.strip()
        line = line.split('\t')
        data = line
        print('skip for year')
        continue
    else :
        data[1] = str(data[1])
    try:
        data[3] = data[3].replace(' ', '9')
        data[3] = re.sub('\W','',data[3])
    except:
        continue
    data[3] = data[3].replace('9', ' ')
    data[3] = data[3].title()
    last_id = data[0]
    data = '\t'.join(data)
    file2.writelines(data)
    line = file1.readline()
    line = line.strip()
    line = line.split('\t')
    data = line
    while last_id == data[0] and len(data[0])>1:
        print('phase_2')
        data[3] = data[3].replace(' ','9')
        data[3] = re.sub('\W', '', data[3])
        data[3] = data[3].replace('9', ' ')
        data[3] = data[3].title()
        file2.writelines(', '+data[3])
        line = file1.readline()
        line = line.strip()
        line = line.split('\t')
        data = line
    file2.writelines('\n')
    print('phase_1')


file1.close()
file2.close()
