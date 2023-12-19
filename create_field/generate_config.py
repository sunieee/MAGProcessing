from utils import *
import yaml


with open('config.yaml') as f:
    configs = yaml.load(f, Loader=yaml.FullLoader)

new_configs = {}

def get_name_by_id(id, typ):
    if typ in ['fieldID', 'children']:
        cursor.execute(f"SELECT name FROM MACG.field_of_study WHERE fieldID='{id}'")
        return cursor.fetchone()[0]
    elif typ == 'JournalID':
        cursor.execute(f"SELECT name FROM MACG.journals WHERE journalID='{id}'")
        return cursor.fetchone()[0]
    elif typ == 'ConferenceID':
        cursor.execute(f"SELECT name FROM MACG.conferences WHERE conferenceID='{id}'")
        return cursor.fetchone()[0]
    return id

for db, config in configs.items():
    new_config = {}
    for typ, lis in config.items():
        key = typ.replace('ID', '').lower()
        lis = [lis] if isinstance(lis, int) or isinstance(lis, str) else lis
        new_config[key] = [get_name_by_id(t, typ) for t in lis]

    order = ['field', 'children', 'journal', 'conference']
    new_configs[db] = {k: new_config[k] for k in order if k in new_config}

# save to example.yaml
with open('example.yaml', 'w') as f:
    yaml.dump(new_configs, f, allow_unicode=True, sort_keys=False)