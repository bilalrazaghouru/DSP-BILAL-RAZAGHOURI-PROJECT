# Streamlit Web Application
**Developer:** [Member 2 Name]

## Features

### Page 1: Make Predictions
- **Single Prediction:** Form-based input for one candidate
- **Batch Prediction:** CSV upload for multiple candidates
- Real-time results display
- Export functionality

### Page 2: Past Predictions
- View historical predictions
- Date range filtering
- Source filtering (webapp/scheduled/all)
- Statistics dashboard

## Running
Windows CMD / PowerShell:
```powershell
cd 04-streamlit-webapp
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

Or use the top-level helper scripts:
```cmd
run_all.bat
```
PowerShell:
```powershell
.\run_all.ps1
```

Access: http://localhost:8501

## Usage
1. Fill in candidate details
2. Click "Get Recommendation"
3. View predicted job and confidence
4. Check past predictions in second page

## Components
- \pp.py\ - Main Streamlit application (4,940 bytes)
