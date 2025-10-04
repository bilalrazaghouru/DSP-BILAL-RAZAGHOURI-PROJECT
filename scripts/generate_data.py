import pandas as pd, numpy as np, os, uuid

os.makedirs("raw-data", exist_ok=True)

def generate_file():
    data = {
        "user_id": np.arange(1, 11),
        "experience": np.random.randint(0, 20, 10),
        "skills_score": np.random.rand(10) * 100
    }
    df = pd.DataFrame(data)
    filename = f"raw-data/data_{uuid.uuid4().hex[:6]}.csv"
    df.to_csv(filename, index=False)
    print(f"Generated {filename}")

if __name__ == "__main__":
    generate_file()
