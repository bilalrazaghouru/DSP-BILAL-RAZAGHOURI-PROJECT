import pandas as pd, os, numpy as np
from sklearn.utils import shuffle

os.makedirs("raw-data", exist_ok=True)

df = pd.read_csv("data/main.csv")
df = shuffle(df)

chunks = np.array_split(df, 10)
for i, c in enumerate(chunks):
    path = f"raw-data/data_part_{i}.csv"
    c.to_csv(path, index=False)
    print(f"✅ Created {path}")
