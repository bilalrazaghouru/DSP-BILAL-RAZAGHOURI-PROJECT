import requests

# Define the FastAPI endpoint
url = "http://127.0.0.1:8000/predict"

# Path to your test CSV file
file_path = "sample.csv"

# Open and send file to the API
with open(file_path, "rb") as f:
    files = {"file": (file_path, f, "text/csv")}
    response = requests.post(url, files=files)

# Display results
if response.status_code == 200:
    print("✅ API Call Successful!")
    print("Response:")
    print(response.json())
else:
    print("❌ API Call Failed!")
    print("Status Code:", response.status_code)
    print("Response:", response.text)
