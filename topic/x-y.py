import pandas as pd

df = pd.read_csv("test.csv", sep=',')
location = []
for i in range(14):
    group = df[df["label"] == i]
    location.append([group['x'].mean(), group['y'].mean()])
df = pd.DataFrame(location, columns=["x", "y"])
root = pd.read_csv("root.csv")
root = root[["Topic", "Count", "Name"]]
df = pd.concat([root, df], axis=1)
print(df)
df.to_csv("root-location.csv", sep='\t', index=False)