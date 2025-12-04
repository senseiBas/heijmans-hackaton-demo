@echo off
echo ========================================
echo Heijmans Demo - Network Diagnostics
echo ========================================
echo.
echo Checking network configuration...
echo.

echo Your IP address:
ipconfig | findstr /i "IPv4"

echo.
echo Checking if port 5000 is listening:
netstat -an | findstr ":5000"

echo.
echo Checking firewall rules for port 5000:
netsh advfirewall firewall show rule name="Heijmans Demo Python Server"

echo.
echo ========================================
echo Quick checklist:
echo ========================================
echo [ ] Python server is running (START_SERVER.bat)
echo [ ] Mobile is on same WiFi/network as laptop
echo [ ] Firewall rule is added (run SETUP_FIREWALL.bat as admin)
echo [ ] Try accessing from mobile: http://10.98.2.112:5000/health
echo.
pause
