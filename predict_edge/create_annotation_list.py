import os
import json
import io

data_dir = "../data/annotated-json-data/"
def main():
    files_to_process = []
    for fname in os.listdir(data_dir):
        if fname.endswith(".json"):
            files_to_process.append(fname)
    print 'Processing %d papers' % (len(files_to_process))
    for fname in files_to_process:
        get_paper_features(fname)

def get_paper_features(fname):
    with io.open(data_dir + '/' + fname,'r',encoding = 'utf-8') as jf:
        annotated_data = json.loads(jf.read())
    paper_id = annotated_data['paper_id']
    for citation_context in annotated_data['citation_contexts']:
        if not 'citation_function' in citation_context:
            continue
        file_new = io.open('../resources/annotated_citfunc_list.txt','a',encoding='utf-8')
        if len(paper_id) < 2:
            paper_id = 'title:'+annotated_data['title']
        file_new.writelines(paper_id+'$'+citation_context['cited_paper_id']+'$'+citation_context['citation_function']+'$'+citation_context['raw_string'].replace('\n','').replace('$','')+'$'+citation_context['cite_context'].replace('\n','').replace('$','')+'\n')
        file_new.close()
if __name__ == "__main__":
    main()

