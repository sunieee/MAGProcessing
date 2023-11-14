import os
import pymysql

database = os.environ.get('database', 'scigene_visualization_field')
if os.environ.get('user') != 'root':
    database = database.replace('scigene', os.environ.get('user'))
    
original_dir = f'../compute_prob/out/{database}'

def create_connection(database):
    conn = pymysql.connect(host='localhost',
                                user=os.environ.get('user'),
                                password=os.environ.get('password'),
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()