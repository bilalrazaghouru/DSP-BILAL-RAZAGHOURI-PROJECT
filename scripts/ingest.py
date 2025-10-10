import os, shutil, random

RAW = "raw-data"
GOOD = "good-data"
os.makedirs(GOOD, exist_ok=True)

def ingest_one_file():
    files = os.listdir(RAW)
    if not files:
        print("No files in raw-data")
        return
    file = random.choice(files)
    shutil.move(os.path.join(RAW, file), os.path.join(GOOD, file))
    print(f"Moved {file} → good-data")

if __name__ == "__main__":
    ingest_one_file()
