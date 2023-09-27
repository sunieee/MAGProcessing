
import pandas as pd
import math

def angle(x, y):
    angle = math.atan2(y, x)
    angle = round(angle * 180 / math.pi, 1)
    if angle < 0:
        angle = 360 + angle
    return angle

df = pd.read_csv("root-location.csv", sep='\t')
df["Topic"].astype(int)
df["Count"].astype(int)
df["Name"] = df["Name"].str.replace("^\d+_\s*", '')
print(df)

data = df.values.tolist()
h = [angle(float(loc[3]), float(loc[4])) for loc in data]
tmp_s = [float(loc[3]) * float(loc[3]) + float(loc[4]) * float(loc[4]) for loc in data]
max_distance = math.sqrt(max(tmp_s))
if max_distance == 0:
    s = [0 for _ in tmp_s]
else:
    s = [math.sqrt(distance) / max_distance for distance in tmp_s]
v = [1 for _ in data]       # TODO 饱和度目前设为1
df["h"] = h
df["s"] = s
df["v"] = v
df.to_csv("root-field.csv", sep='\t', index=False)
