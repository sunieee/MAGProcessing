import pandas as pd
import math
import json
import numpy as np

authorID = '2125104194'
basedir = '../create_field/out/fellow/'

'''
papers = await d3.csv(`../out/papers/${authorID}.csv`);
links = await d3.csv(`../out/links/${authorID}.csv`);
field_leaves = await d3.csv('field_leaves.csv');
paperID2topic = await d3.json('paperID2topic.json');
'''
papers = pd.read_csv(f'{basedir}/papers/' + authorID + '.csv')
papers['paperID'] = papers['paperID'].astype(str)
papers['citationCountByYear'] = papers['citationCountByYear'].astype(str)

papers = papers[papers['isKeyPaper'] > 0.5]
papers = papers[~papers['title'].str.contains(r'survey|surveys', case=False, regex=True)]
nodes = set(papers['paperID'])
MIN_YEAR = papers['year'].min()

links = pd.read_csv(f'{basedir}/links/' + authorID + '.csv')
links['parentID'] = links['parentID'].astype(str)
links['childrenID'] = links['childrenID'].astype(str)
links = links[links['extendsProb'] > 0.5]
links = links[links['parentID'].isin(nodes) & links['childrenID'].isin(nodes)]
connected_nodes = set(links['parentID']) | set(links['childrenID'])

papers = papers[papers['paperID'].isin(connected_nodes)]

field_leaves = pd.read_csv('field_leaves.csv')
# paperID2topic = pd.read_json('paperID2topic.json')
with open('paperID2topic.json', 'r') as f:
    paperID2topic = json.load(f)

def hsvToRgb(h, s, v):
    c = v * s
    x = c * (1 - abs(((h / 60) % 2) - 1))
    m = v - c
    r, g, b = 0, 0, 0

    if h >= 0 and h < 60:
        r, g, b = c, x, 0
    elif h >= 60 and h < 120:
        r, g, b = x, c, 0
    elif h >= 120 and h < 180:
        r, g, b = 0, c, x
    elif h >= 180 and h < 240:
        r, g, b = 0, x, c
    elif h >= 240 and h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x

    rgbColor = [(r + m) * 255, (g + m) * 255, (b + m) * 255]
    return rgbColor

def hsvToHex(h, s, v):
    rgbColor = hsvToRgb(h, s, v)
    r = round(rgbColor[0])
    g = round(rgbColor[1])
    b = round(rgbColor[2])
    # 将RGB值转换为十六进制字符串
    toHex = lambda x: "0" + hex(x)[2:] if len(hex(x)[2:]) == 1 else hex(x)[2:]
    return "#" + toHex(r) + toHex(g) + toHex(b)

topic2color = {}
for d in field_leaves.to_records():
    topic2color[d['Topic']] = hsvToHex(d['h'], 0.4, d['v'])
print('topic2color', topic2color)

data = {
    'nodes': [],
    'edges': []
}

for link in links.to_records():
    data['edges'].append({
        'source': link['childrenID'],
        'target': link['parentID'],
        'extendsProb': link['extendsProb'],
    })


def isNone(x):
    if x is None or x == '' or x == 'nan':
        return True
    try:
        return np.isnan(x)
    except TypeError:
        return False
    
def relu(x):
    return 0 if isNone(x) else int(x)

for paper in papers.to_records():
    if isNone(paper['citationCountByYear']):
        cumulativeCitationsByYear = []
    else:
        citations = [int(count) for count in paper['citationCountByYear'].split(',')]
        cumulativeCount = 0
        cumulativeCitationsByYear = [cumulativeCount + count for count in citations]
    data['nodes'].append({
        'id': paper['paperID'],
        'title': paper['title'],
        'isKeyPaper': paper['isKeyPaper'],
        'year': paper['year'],
        'citationCount': relu(paper['citationCount']),
        'citeStartYear': relu(paper['citeStartYear']),
        'citationCountByYear': cumulativeCitationsByYear,
        'radius': 2 + np.cbrt(relu(paper['citationCount'])),
        'topic': paperID2topic[paper['paperID']],
        'color': topic2color[paperID2topic[paper['paperID']]] or '#000000',
        'level': paper['year'] - MIN_YEAR + 1,
    })

class npEncoder(json.JSONEncoder):
    """ Special json encoder for np types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (np.bool_,)):
            return bool(obj)
        return json.JSONEncoder.default(self, obj)

with open(f'{authorID}.json', 'w') as f:
    json.dump(data, f, cls = npEncoder)