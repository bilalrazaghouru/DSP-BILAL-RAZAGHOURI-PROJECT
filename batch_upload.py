import os
import requests
import json
import shutil

# API endpoint
url = "http://127.0.0.1:8000/predict"

# Folders
raw_data = "raw-data"
good_data = "good-data"
response_folder = "responses"

# Ensure folders exist
os.makedirs(raw_data, exist_ok=True)
os.makedirs(good_data, exist_ok=True)
os.makedirs(response_folder, exist_ok=True)

# Process each CSV file in raw-data/
for filename in os.listdir(raw_data):
    if filename.endswith(".csv"):
        file_path = os.path.join(raw_data, filename)
        print(f"📤 Uploading: {filename}")

        with open(file_path, "rb") as f:
            files = {"file": (filename, f, "text/csv")}
            response = requests.post(url, files=files)

        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success: {result}")

            # Save JSON response
            out_file = os.path.join(response_folder, f"{filename}_response.json")
            with open(out_file, "w") as out:
                json.dump(result, out, indent=4)

            # Move file to good-data/
            dest_path = os.path.join(good_data, filename)
            shutil.move(file_path, dest_path)
            print(f"📦 Moved {filename} → good-data/")
        else:
            print(f"❌ Failed for {filename}: {response.status_code}")
