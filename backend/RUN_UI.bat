@echo off
echo ========================================
echo 🌐 DATA QUALITY FRAMEWORK - UI MODE
echo ========================================
echo.
echo ⚠️  IMPORTANT: This mode is for FRONTEND/API use
echo    The API will accept dynamic database credentials
echo    from React/Node.js frontend applications
echo.
echo 📝 For Ananya (Frontend Developer):
echo    - API runs on: http://localhost:5000
echo    - Use POST /api/check-single with JSON body
echo    - See example_requests.txt for samples
echo.
echo 📝 Logs will be saved to the 'logs' folder
echo.
echo ========================================
echo.

REM Always run in dynamic mode for UI
set DQ_USE_CONFIG_FILE=false

REM Ensure logs directory exists
if not exist logs mkdir logs

echo Starting API Server...
echo Press Ctrl+C to stop
echo.
echo 📍 API Logs: logs\dq_api_*.log
echo 📍 API URL: http://localhost:5000
echo.
python run_api.py

pause