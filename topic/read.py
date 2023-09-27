import pandas as pd

df = pd.read_csv("root-field.csv", sep=',')
print(df.values.tolist())