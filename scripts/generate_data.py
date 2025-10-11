# scripts/generate_data.py
import argparse, os, numpy as np, pandas as pd
from sklearn.utils import shuffle

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--outdir", default="raw-data")
    ap.add_argument("--num-files", type=int, default=10)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = shuffle(pd.read_csv(args.dataset), random_state=42)
    chunks = np.array_split(df, args.num_files)
    for i, c in enumerate(chunks):
        path = os.path.join(args.outdir, f"data_part_{i}.csv")
        c.to_csv(path, index=False)
        print(f"✅ Created {path}")

if __name__ == "__main__":
    main()
