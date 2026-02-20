@echo off
echo Installing Data Quality Framework...

REM -----------------------------
REM 1. Install Python packages
REM -----------------------------
echo Installing Python packages...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo Installation complete!
echo Run: python main.py
pause

