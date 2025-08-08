@echo off
cd /d "%~dp0"
pip install pyinstaller
pyinstaller --onefile --windowed --name TZScraper Tanzania_Maps_Scraper.py
echo Built dist\TZScraper.exe
pause
