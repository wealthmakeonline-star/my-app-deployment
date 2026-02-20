@echo off
echo ========================================
echo 🚀 DATA QUALITY FRAMEWORK - CLI MODE
echo ========================================
echo.
echo Select Mode:
echo 1. Dynamic Input (Enter credentials manually)
echo 2. Config File Mode (Use db_config.py)
echo 3. Exit
echo.

set /p choice="Enter choice (1-3): "

if "%choice%"=="1" (
    echo.
    echo ⚙️  Starting in DYNAMIC INPUT mode...
    echo    You will enter credentials manually
    set DQ_USE_CONFIG_FILE=false
    set DQ_SKIP_CONNECTION_PROMPT=true
    goto :run_main
) else if "%choice%"=="2" (
    echo.
    echo 📁 Starting in CONFIG FILE mode...
    echo    Using credentials from db_config.py
    set DQ_USE_CONFIG_FILE=true
    set DQ_SKIP_CONNECTION_PROMPT=true
    goto :run_main
) else if "%choice%"=="3" (
    echo.
    echo 👋 Goodbye!
    pause
    exit /b
) else (
    echo.
    echo ❌ Invalid choice!
    pause
    exit /b
)

:run_main
echo.
echo 📊 Starting Data Quality Framework...
echo 📝 Logs will be saved to the 'logs' folder
echo ========================================
echo.

REM Ensure logs directory exists
if not exist logs mkdir logs

python main.py

pause