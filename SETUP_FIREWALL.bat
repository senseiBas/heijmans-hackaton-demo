@echo off
echo ========================================
echo Heijmans Demo - Firewall Setup
echo ========================================
echo.
echo Dit script opent poort 5000 in Windows Firewall
echo zodat andere devices toegang hebben tot de server.
echo.
echo Klik met rechtermuisknop en kies "Run as Administrator"
echo.
pause

netsh advfirewall firewall add rule name="Heijmans Demo Python Server" dir=in action=allow protocol=TCP localport=5000

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS! Firewall regel toegevoegd
    echo ========================================
    echo.
    echo Poort 5000 is nu toegankelijk vanaf het netwerk
    echo Je kunt nu de Python server starten.
    echo.
) else (
    echo.
    echo ========================================
    echo FOUT: Kon firewall regel niet toevoegen
    echo ========================================
    echo.
    echo Zorg dat je dit script als Administrator draait!
    echo.
)

pause
