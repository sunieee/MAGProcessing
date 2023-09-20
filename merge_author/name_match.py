from gensim import utils
import pandas as pd
from Levenshtein import ratio as levenSim
import os
import unicodedata
import re
import numpy
from tqdm import tqdm
import time
import json
import pandas as pd
import gensim
from gensim.parsing.preprocessing import preprocess_string
gensim.parsing.preprocessing.STOPWORDS = set()
def strip_short2(s, minsize=1):
    s = utils.to_unicode(s)         #hajičová在数据库里是hajicova,因此没匹配上'Eva Hajičová'
    s = ''.join(char for char in unicodedata.normalize('NFKD', s) if not unicodedata.combining(char))
    def remove_short_tokens(tokens, minsize):
        return [token for token in tokens if len(token) >= minsize]
    return " ".join(remove_short_tokens(s.split(), minsize))
gensim.parsing.preprocessing.DEFAULT_FILTERS[6]=strip_short2
del gensim.parsing.preprocessing.DEFAULT_FILTERS[-1]

n1="Shrikanth S. Narayanan"
n2="satoshi sekine"
n3="Rose jeff"
tb1=[n1,n2,n3]
tb2=[n3,n2,n1]


'''compare name1 and name2, return similarity'''
def compare_name(n1,n2, levensimrate=0.7):
    n1_tb = preprocess_string(re.sub('[^\s\w]', "", n1))
    n2_tb = preprocess_string(re.sub('[^\s\w]', "", n2))
    n1_tb=sorted(n1_tb,key = lambda i:len(i),reverse=True)
    n2_tb = sorted(n2_tb, key=lambda i: len(i), reverse=True)
    if set(n1_tb).issubset(set(n2_tb)) or set(n2_tb).issubset(set(n1_tb)):
        return 1.0
    n1_py=[]
    n2_py=[]
    pinyinflag1=True
    pinyinflag2=True
    for word in n1_tb:
        tb,flag=parse_pinyin2(word)
        pinyinflag1 = (pinyinflag1 and flag) if len(word)!=1 else pinyinflag1
        n1_py.extend(tb)
    for word in n2_tb:
        tb, flag = parse_pinyin2(word)
        pinyinflag2 = (pinyinflag2 and flag) if len(word)!=1 else pinyinflag2
        n2_py.extend(tb)
    pinyinflag = pinyinflag1 and pinyinflag2
    if (len(n1_py)!=len(n1_tb) or len(n2_py)!=len(n2_tb)) and len(n1_py)==len(n2_py) and set(n1_py)==set(n2_py) and pinyinflag:
        return 1.0
    penalty_1=len(n1_tb)
    penalty_2=len(n2_tb)
    n1_rest=n1_tb.copy()
    for word1 in n1_tb:
        rmflag = ''
        for word2 in n2_tb:
            if min(len(word1),len(word2)) == 1 and (word1.startswith(word2) or word2.startswith(word1)) and pinyinflag1==pinyinflag2:
                rmflag=word2
                penalty_1-=0.9
                penalty_2-=0.9
                break
            elif parse_pinyin2(word1)[1] and parse_pinyin2(word2)[1]:
                if word1==word2:
                    penalty_1 -= 1
                    penalty_2 -= 1
                pass
            elif levenSim(word1, word2) >= levensimrate:
                rmflag=word2
                penalty_1-=levenSim(word1,word2)
                penalty_2-=levenSim(word1,word2)
                break
        try:
            n2_tb.remove(rmflag)
            n1_rest.remove(word1)
        except:
            pass
    n2_rest = n2_tb.copy()
    restword1=''.join(n1_rest)
    restword2=''.join(n2_rest)
    if levenSim(restword1, restword2) >= 0.9 and (not parse_pinyin2(restword1)[1] or not parse_pinyin2(restword2)[1]):
        penalty_1 -= levenSim(restword1, restword2)
        penalty_2 -= levenSim(restword1, restword2)
    penalty=min(penalty_1,penalty_2)+1
    return min(1.0, float(1/penalty))


def compare_nametb(tb1,tb2):
    tb1_dict={ele:[0,[]] for ele in tb1}
    tb2_dict={ele:[0,[]] for ele in tb2}
    for n1 in tb1:
        for n2 in tb2:
            similarity=compare_name(n1, n2)

            if similarity>tb1_dict[n1][0]:
                tb1_dict[n1][1] = [n2]
                tb1_dict[n1][0] = similarity
            elif similarity==tb1_dict[n1][0] and n2 not in tb1_dict[n1][1]:
                tb1_dict[n1][1].append(n2)

            if similarity>tb2_dict[n2][0]:
                tb2_dict[n2][1] = [n1]
                tb2_dict[n2][0] = similarity
            elif similarity==tb2_dict[n2][0] and n1 not in tb2_dict[n2][1]:
                tb2_dict[n2][1].append(n1)
    return tb1_dict,tb2_dict

def dump_matchdict(path,matchdict):
    data=[]
    for line in matchdict.items():
        data.append([line[1][0],line[0],'|'.join(line[1][1])])
    df=pd.DataFrame(data,columns=["similarity","host_name","matcher_name"])
    df=df.sort_values(by="similarity",ascending=False)
    df.to_csv(path+'.csv',index=None)
    return df

def parse_pinyin2(word):
    w=word
    output=[]
    finals = ['a','o','e','i','u','v','ai','ei','ui','ao','ou','iu','ie','ue','ve','an','en','in','un','vn','ang','eng','ing','ong','iang','uang','uan','ua','ian']
    initials = ['b','p','m','f','d','t','n','l','g','k','h','j','q','x','zh','ch','sh','r','z','c','s','y','w']
    pinyins = ['zhi','chi','shi','ri','zi','ci','si','yi','wu','yu','ye','yue','yuan','yin','yun','ying']
    for initial in initials:
        for final in finals:
            pinyin=initial+final
            pinyins.append(pinyin)
    pinyins=set(pinyins)
    flag_pinyin=True
    for i in range(1,3):
        for i in range(min(6,len(w)),1,-1):
            if w[0:i] in pinyins:
                output.append(w[0:i])
                break
        if len(output)==0:
            flag_pinyin = False
            output = [word]
            break
        elif i==len(w):
            break
        else:
            w=w[i:]
    if ''.join(output) != word:
        output=[word]
        flag_pinyin = False
    return output,flag_pinyin


def levenshtein_distance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for index2, char2 in enumerate(s2):
        new_distances = [index2 + 1]
        for index1, char1 in enumerate(s1):
            if char1 == char2:
                new_distances.append(distances[index1])
            else:
                new_distances.append(1 + min((distances[index1], distances[index1 + 1], new_distances[-1])))
        distances = new_distances

    return distances[-1]

def test():
    # s1 = 'J. J. van Wijk'
    # s2 = 'Jarke J. van Wijk'
    s1 = "kitten"
    s2 = "sitting"

    t = time.time()
    for i in range(100):
        compare_name(s1, s2)
    print('time', time.time() - t)

    t = time.time()
    for i in range(100):
        levenshtein_distance(s1, s2) # / max(len(s1), len(s2))
    print('time', time.time() - t)


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, (numpy.int_, numpy.intc, numpy.intp, numpy.int8,
                            numpy.int16, numpy.int32, numpy.int64, numpy.uint8,
                            numpy.uint16, numpy.uint32, numpy.uint64)):
            return int(obj)
        elif isinstance(obj, (numpy.float_, numpy.float16, numpy.float32,
                              numpy.float64)):
            return float(obj)
        elif isinstance(obj, (numpy.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (numpy.bool_,)):
            return bool(obj)
        return json.JSONEncoder.default(self, obj)
    

if __name__ == "__main__":
    df = pd.read_csv('top_field_authors.csv', sep=',', header=None)
    df.columns = ['id', 'AuthorId', 'name', '#paper', '#citation', 'hIndex', 'rank'] + list(range(3))
    print(df)
    author_ranks = df['rank']
    author_names = df['name']
    lev_file = 'lev.json'

    if os.path.exists(lev_file):
        with open (lev_file, 'r') as f:
            lev_lis = json.load(f)
    else:
        lev_dic = {}
        for i in tqdm(range(len(author_names))):
            for j in range(len(author_names)):
                if i < j:
                    lev_dic[(i,j)] = levenshtein_distance(author_names[i], author_names[j]) / (len(author_names[i]) + len(author_names[j]))
        # sort lev_dic by values
        lev_lis = list(sorted(lev_dic.items(), key=lambda item: item[1]))

        # save dic to local
        with open('lev.json', 'w') as f:
            json.dump(lev_lis, f, cls=NumpyEncoder)
    
    match_groups = pd.DataFrame(columns=['ix1', 'ix2', 'rank1', 'rank2', 'name1', 'name2', 'lev_dis', 'similarity'])
    
    for group in tqdm([g for g in lev_lis if g[1] <= 0.3]):
        ix1, ix2 = group[0]
        similarity = compare_name(author_names[ix1], author_names[ix2])
        match_groups.loc[len(match_groups)] = {
            'ix1': ix1,
            'ix2': ix2,
            'rank1': author_ranks[ix1],
            'rank2': author_ranks[ix2],
            'name1': author_names[ix1],
            'name2': author_names[ix2],
            'lev_dis': group[1],
            'similarity': similarity
        }

    match_groups.to_csv('match_groups.csv', encoding='UTF-8')