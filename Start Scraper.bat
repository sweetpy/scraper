@echo off
cd /d "%~dp0"
:loop
python "Tanzania_Maps_Scraper.py"
echo [INFO] Process exited. Restarting in 30s...
timeout /t 30 >nul
goto loop
