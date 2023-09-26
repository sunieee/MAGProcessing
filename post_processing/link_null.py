import pandas as pd
import os

directory = "online/"

files = os.listdir(directory)
for file in files:
    if file.startswith("links_"):
        links = pd.read_csv(os.path.join(directory, file), sep=',', index_col=0)
        is_column_a_empty = links['citationContext'].isna().all()
        if is_column_a_empty and len(links) != 0:
            print(file)
            links["citationContext"] = "Missing"
            links.to_csv("link/" + file, sep=',')
            
