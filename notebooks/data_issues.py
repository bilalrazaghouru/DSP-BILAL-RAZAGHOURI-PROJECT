import pandas as pd, numpy as np, os

os.makedirs("raw-data", exist_ok=True)
df = pd.read_csv("data/main.csv")

# Introduce 7 issues
df_missing = df.copy()
df_missing.iloc[0, 0] = None

df_invalid = df.copy()
df_invalid.iloc[0, 1] = "INVALID"

df_negative = df.copy()
df_negative.iloc[0, 2] = -99

df_duplicate = pd.concat([df, df.iloc[:5]])

df_unknown = df.copy()
df_unknown.iloc[0, 3] = "UnknownCountry"

df_strnum = df.copy()
df_strnum.iloc[0, 4] = "string instead of number"

df_outlier = df.copy()
df_outlier.iloc[0, 5] = 999999

for i, d in enumerate([df_missing, df_invalid, df_negative, df_duplicate, df_unknown, df_strnum, df_outlier]):
    d.to_csv(f"raw-data/error_type_{i}.csv", index=False)
print("✅ Generated all data issue files.")
