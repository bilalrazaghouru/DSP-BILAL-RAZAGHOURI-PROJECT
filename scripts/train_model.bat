@echo off
REM Helper to run the model training within the repo venv
cd /d "%~dp0\02-ml-model\models"
if not exist ..\..\.venv (
  python -m venv ..\..\.venv
)
call ..\..\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r ..\requirements.txt || echo 'No requirements.txt in 02-ml-model; skipping'
python train_model.py
pause
