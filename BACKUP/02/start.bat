@echo off
title RMQ QA Utility
echo.
echo  ============================================
echo   RMQ QA Utility  -  Starting...
echo  ============================================
echo.

REM Check Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Install Python 3.10+ and add to PATH.
    pause
    exit /b 1
)

REM Install dependencies if needed
echo  Checking dependencies...
pip install -r requirements.txt --quiet

echo.
echo  Starting web server...
echo  Open browser at: http://localhost:8080
echo  Press Ctrl+C to stop.
echo.

python app.py

pause
