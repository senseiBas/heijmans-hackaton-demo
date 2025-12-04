@echo off
echo ========================================
echo Heijmans Databricks Proxy Server
echo ========================================
echo.
echo Installing dependencies...
pip install -r requirements.txt
echo.
echo Starting server...
echo.
python databricks_proxy.py
pause
