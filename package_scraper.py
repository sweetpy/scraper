import os, zipfile

ROOT = os.path.abspath(".")
OUT = os.path.join(os.path.dirname(ROOT), "Tanzania_Maps_Scraper_Kit.zip")
INCLUDE = [
    "Tanzania_Maps_Scraper.py",
    "Start Scraper.bat",
    "requirements.txt",
    "README.txt",
    "proxies.txt",
    "build_exe.bat",
    "TaskScheduler_TZScraper.xml"
]
EXTRA_DIRS = ["exports", "wheelhouse"]

def add_dir(z, folder):
    for root, _, files in os.walk(folder):
        for f in files:
            p = os.path.join(root, f)
            z.write(p, os.path.relpath(p, ROOT))

with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
    for f in INCLUDE:
        p = os.path.join(ROOT, f)
        if os.path.exists(p):
            z.write(p, os.path.relpath(p, ROOT))
    for d in EXTRA_DIRS:
        p = os.path.join(ROOT, d)
        if os.path.isdir(p): add_dir(z, p)

print(f"[DONE] {OUT}")
