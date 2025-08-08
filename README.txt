Tanzania Maps Scraper — Local-Only Research Kit (Dedupe-enabled)
===============================================================

Prereqs (first time only)
1) Install Python 3.11+ (add to PATH)
2) Open CMD in this folder:
   pip install -r requirements.txt
   python -m textblob.download_corpora

Run (no CMD needed)
- Double-click Start Scraper.bat (auto-retry loop)
- Or double-click Tanzania_Maps_Scraper.py (GUI)

Dashboard (local)
- http://127.0.0.1:8000/
  Tabs: Overview, Regions, Categories, Logs, Settings
  Actions: Export Now, Backup DB, Download DB

Persistence / Resume
- Auto checkpointing; resumes where it stopped after power/internet loss.
- DB: bizinteltz.db (SQLite, WAL mode)
- Logs: scrape_progress.log (rotating)
- Exports: /exports (CSV/JSON/XLSX by region)

Duplicates
- Robust dedupe: stable signature on (norm_name | norm_address | phone | website); UPSERT on conflict.
- Soft dedupe maintenance cleans legacy duplicates by (norm_name, norm_address).

Optional
- proxies.txt for proxy rotation (one URL per line)
- build_exe.bat to produce a single EXE with PyInstaller
- TaskScheduler_TZScraper.xml to autostart at login + daily 03:00

Offline Install
- On internet PC:
  pip download -r requirements.txt -d wheelhouse
- On target PC:
  python -m pip install --no-index --find-links=wheelhouse -r requirements.txt
