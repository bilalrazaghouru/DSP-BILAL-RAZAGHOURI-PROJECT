import os
import random
import shutil
import time

RAW_DIR = "raw-data"
GOOD_DIR = "good-data"

# Ensure folders exist
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(GOOD_DIR, exist_ok=True)

def read_data():
    """Simulate Airflow task: randomly pick one file from raw-data"""
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
    if not files:
        print("❌ No files found in raw-data/. DAG run skipped.")
        return None

    file = random.choice(files)
    file_path = os.path.join(RAW_DIR, file)
    print(f"📥 Picked file: {file_path}")
    return file_path

def save_file(file_path):
    """Simulate Airflow task: move file to good-data"""
    if not file_path:
        print("⏭️ No file to move, skipping.")
        return

    file_name = os.path.basename(file_path)
    dest_path = os.path.join(GOOD_DIR, file_name)
    shutil.move(file_path, dest_path)
    print(f"📦 File moved from raw-data → good-data: {file_name}")

def simulate_dag_run():
    print("\n🚀 Starting simulated Airflow ingestion DAG...")
    file_path = read_data()
    if file_path:
        time.sleep(1)  # Simulate task delay
        save_file(file_path)
    print("✅ DAG run complete.\n")

if __name__ == "__main__":
    simulate_dag_run()
