from utils import *


# src_db = 'scigene_visualization_field'
# dst_db = 'scigene_visualization_field_old'

src_db = 'scigene_visualization2_field'
dst_db = 'scigene_visualization_field'

cursor.execute(f'create database if not exists {dst_db}')
 # Show tables in MAG
cursor.execute(f"SHOW TABLES IN {src_db}")
tables = cursor.fetchall()
tables = [table[0] for table in tables]
print("Tables in MAG:", tables)


for table in tables:
    cursor.execute(f"RENAME TABLE {src_db}.{table} TO {dst_db}.{table};")

# Show tables in MACG
cursor.execute(f"SHOW TABLES IN {dst_db};")
tables = cursor.fetchall()
print(f"Tables in {dst_db}:", tables)

# Drop database MAG
cursor.execute(f"DROP DATABASE {src_db};")

conn.commit()