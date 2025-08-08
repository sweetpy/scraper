# Tanzania Business Intelligence Scraper
# ================================
# Scrapes Google Maps for ALL types of businesses in Tanzania (formal & informal)
# Saves semantically enriched data into SQLite DB with resilience for long-term scraping
# Offline, standalone only. No cloud. Research use only.

import time
import random
import pandas as pd
import traceback
import os
import logging
from logging.handlers import RotatingFileHandler
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException
import undetected_chromedriver as uc
from fake_useragent import UserAgent
import sqlite3
import schedule
import datetime
import json
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn
import threading
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import re
from collections import Counter
import textblob
import geopy
from geopy.geocoders import Nominatim
import sys
import argparse
import signal
import tkinter as tk
import webbrowser
from pathlib import Path
import hashlib
from urllib.parse import urlparse, urlunparse

# -----------------------
# CONFIGURATION (defaults; overridable via CLI)
# -----------------------
SEED_TERMS = [
    "duka", "huduma", "fundi", "mgahawa", "saluni", "machinga", "boda boda", "kibanda", "hospitali", "chuo"
]

SEARCH_TYPES_FILE = "dynamic_search_types.json"
SCRAPE_LOG = "scrape_progress.log"
DB_FILE = "bizinteltz.db"
EXPORT_DIR = "exports"
HEADLESS = True
COORD_GRID_STEP = 0.18  # degrees; lower = denser grid (safer default for long runs)
SCROLL_LIMIT = 20
MAX_PER_COORD = 90
ACTION_PAUSE = (3.5, 8.0)
COOLDOWN_BETWEEN_COORDS = (40, 90)
RELAUNCH_EVERY = 180  # relaunch browser after N business clicks
CHROMEDRIVER_PATH = None  # e.g. r"C:\\WebDriver\\bin\\chromedriver.exe" or None
PROXY_LIST_FILE = None     # e.g. "proxies.txt" with lines http://user:pass@host:port
ROTATE_PROXY_EVERY = 30    # rotate proxy after N coords
RUN_AT = "03:00"           # default daily run time
TERM_FILTER = None         # run only terms containing this substring
REGION_FILTER = None       # export or API filter hint
EXPORT_FORMATS = ["csv", "json", "xlsx"]
RUN_MAX_HOURS = None       # optional auto-stop

STOP_FLAG = False

geolocator = Nominatim(user_agent="tzbiz_scraper")

# -----------------------
# LOGGING SETUP
# -----------------------
logger = logging.getLogger("tzscraper")
logger.setLevel(logging.INFO)
rot = RotatingFileHandler(SCRAPE_LOG, maxBytes=5*1024*1024, backupCount=7, encoding="utf-8")
rot.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
if not logger.handlers:
    logger.addHandler(rot)
console = logging.StreamHandler(sys.stdout)
console.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
logger.addHandler(console)

def live_progress(msg):
    logger.info(msg)

# -----------------------
# DYNAMIC SEARCH TYPE BUILDER
# -----------------------
def fetch_related_terms(term):
    """Fetch related search terms using Google's suggestion API.

    The previous implementation scraped the entire search results page which
    returned a lot of unrelated text fragments (e.g. "councilhttps").  As a
    consequence the dynamically generated search term list contained junk
    tokens and the scraper would query Google Maps with meaningless phrases.
    This function now leverages the public suggestion endpoint which returns a
    clean JSON payload of relevant search suggestions.
    """
    headers = {"User-Agent": UserAgent().random}
    try:
        params = {"client": "firefox", "hl": "sw", "q": term}
        resp = requests.get(
            "https://suggestqueries.google.com/complete/search",
            params=params,
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return [s.lower() for s in data[1] if isinstance(s, str)]
    except (RequestException, ValueError) as e:
        logger.warning(f"Suggest fetch error for '{term}': {e}")
        return []


def build_dynamic_search_types():
    all_terms = set()
    word_freq = Counter()
    for seed in SEED_TERMS:
        terms = fetch_related_terms(seed)
        live_progress(f"Seed '{seed}': {len(terms)} suggestions")
        for term in terms:
            tokens = re.findall(r'[a-zA-Z]{3,}', term)
            word_freq.update(tokens)
            all_terms.update(tokens)
        time.sleep(random.uniform(1, 2))
    if not all_terms:
        live_progress("No suggestions fetched; keeping existing dynamic search types")
        try:
            with open(SEARCH_TYPES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    filtered = [w for w in word_freq if word_freq[w] > 1]
    all_terms = sorted(set(filtered))
    with open(SEARCH_TYPES_FILE, "w", encoding="utf-8") as f:
        json.dump(all_terms, f, ensure_ascii=False, indent=2)
    live_progress(f"Saved {len(all_terms)} dynamic search terms")
    return all_terms

# -----------------------
# LOAD SEARCH TYPES (Static + Dynamic Merge)
# -----------------------
STATIC_TERMS = [
    "restaurant", "pharmacy", "supermarket", "clinic", "hospital", "bank", "atm",
    "cafe", "hotel", "lodge", "bar", "school", "university", "gas station", "car repair",
    "hardware store", "electronics", "mobile shop", "boutique", "grocery store",
    "bakery", "butchery", "furniture store", "salon", "barber", "tailor", "mechanic",
    "bookshop", "stationery", "printing", "signboard", "shoes", "bags", "market",
    "fish market", "meat market", "fruit vendor", "vegetable stall", "juice bar",
    "fitness center", "gym", "car wash", "welding", "painting service",
    "interior design", "plumber", "electrician", "barbershop", "laundry",
    "internet cafe", "mobile money", "insurance", "microfinance", "saccos",
    "church", "mosque", "temple", "construction", "spare parts", "auto parts",
    "matatu stage", "daladala", "hairdresser", "video editing", "cyber cafe",
    "computer repair", "home tutor", "duka", "mama lishe", "fundi", "boda boda",
    "kibanda", "machinga", "vinyago", "saluni", "mgahawa", "mpesa", "tigopesa",
    "halopesa", "mbao", "viatu", "nguo", "kiatu", "pikipiki", "bajaji", "kioski",
    "barafu", "mwalimu binafsi", "chakula", "matunda", "nafaka", "maziwa", "dengu",
    "harusi", "mapambo"
]

def load_search_types():
    try:
        with open(SEARCH_TYPES_FILE, "r", encoding="utf-8") as f:
            dynamic_terms = json.load(f)
    except Exception:
        dynamic_terms = []
    combined = sorted(set(STATIC_TERMS + dynamic_terms))
    if TERM_FILTER:
        combined = [t for t in combined if TERM_FILTER.lower() in t.lower()]
    live_progress(f"Search types loaded: {len(combined)}")
    return combined

# -----------------------
# EMAIL + REVIEW EXTRACTION
# -----------------------
def extract_email(text):
    try:
        emails = re.findall(r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}", text)
        return emails[0] if emails else None
    except Exception as e:
        logger.error(f"Email extraction error: {e}")
        return None

def extract_google_reviews(page_source):
    try:
        soup = BeautifulSoup(page_source, "html.parser")
        reviews_text = " ".join(t for t in soup.stripped_strings if isinstance(t, (str, NavigableString)))
        reviews_text = reviews_text.lower()
        blob = textblob.TextBlob(reviews_text[:500])
        return {
            "summary": reviews_text[:300],
            "sentiment": blob.sentiment.polarity
        }
    except Exception as e:
        logger.error(f"Review parse error: {e}")
        return {"summary": None, "sentiment": None}

# -----------------------
# DB INIT / DEDUPE
# -----------------------
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cur = conn.cursor()
cur.execute("""PRAGMA journal_mode=WAL;""")
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS businesses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        category TEXT,
        rating TEXT,
        address TEXT,
        phone TEXT,
        website TEXT,
        email TEXT,
        search_term TEXT,
        latitude REAL,
        longitude REAL,
        region TEXT,
        review_summary TEXT,
        review_sentiment REAL,
        ts TEXT,
        norm_name TEXT,
        norm_address TEXT,
        dedupe_key TEXT
    )
    """
)
# Backfill columns if upgrading existing DB
for col in ["norm_name","norm_address","dedupe_key"]:
    try:
        cur.execute(f"ALTER TABLE businesses ADD COLUMN {col} TEXT")
    except Exception:
        pass
cur.execute("""CREATE INDEX IF NOT EXISTS idx_businesses_region ON businesses(region);""")
cur.execute("""CREATE INDEX IF NOT EXISTS idx_businesses_phone ON businesses(phone);""")
cur.execute("""CREATE INDEX IF NOT EXISTS idx_businesses_website ON businesses(website);""")
cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_businesses_dedupe ON businesses(dedupe_key);""")
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS checkpoints (
        id INTEGER PRIMARY KEY,
        term_idx INTEGER,
        lat REAL,
        lng REAL,
        coord_idx INTEGER,
        processed INTEGER DEFAULT 0,
        updated_at TEXT
    )
    """
)
conn.commit()

# -----------------------
# GRID GEN (Tanzania approx bbox)
# -----------------------
# South lat ~ -11.8, North ~ -1.0; West lon ~ 29.2, East ~ 40.6
lat_range = (-11.8, -1.0)
lon_range = (29.2, 40.6)

def generate_coords(step=COORD_GRID_STEP):
    lat = lat_range[0]
    coords = []
    while lat <= lat_range[1]:
        lon = lon_range[0]
        while lon <= lon_range[1]:
            coords.append((round(lat, 4), round(lon, 4)))
            lon = round(lon + step, 4)
        lat = round(lat + step, 4)
    return coords

COORDS = generate_coords()

# -----------------------
# Browser management + proxies
# -----------------------
click_counter = 0
proxy_use_count = 0
current_proxy = None

def load_proxies():
    if not PROXY_LIST_FILE or not os.path.exists(PROXY_LIST_FILE):
        return []
    with open(PROXY_LIST_FILE, 'r') as f:
        proxies = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
    random.shuffle(proxies)
    return proxies

PROXIES = load_proxies()

def next_proxy():
    global PROXIES, current_proxy, proxy_use_count
    if not PROXIES:
        current_proxy = None
        return None
    current_proxy = PROXIES.pop(0)
    PROXIES.append(current_proxy)
    proxy_use_count = 0
    return current_proxy

def launch_browser(force_new_proxy=False):
    global click_counter, proxy_use_count, current_proxy
    click_counter = 0
    if force_new_proxy or (ROTATE_PROXY_EVERY and proxy_use_count >= ROTATE_PROXY_EVERY):
        next_proxy()
    for attempt in range(5):
        try:
            options = uc.ChromeOptions()
            if HEADLESS:
                options.add_argument("--headless=new")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(f"user-agent={UserAgent().random}")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            if current_proxy:
                options.add_argument(f"--proxy-server={current_proxy}")
                live_progress(f"Using proxy: {current_proxy}")
            driver = uc.Chrome(options=options, driver_executable_path=CHROMEDRIVER_PATH)
            driver.set_page_load_timeout(45)
            driver.implicitly_wait(8)
            return driver, ActionChains(driver)
        except WebDriverException as e:
            live_progress(f"Browser launch failed (attempt {attempt+1}): {e}")
            time.sleep(15)
    raise RuntimeError("Failed to launch browser after retries")

# -----------------------
# Helpers
# -----------------------
def normalize_name(name: str) -> str:
    if not name: return ""
    n = name.lower().strip()
    n = re.sub(r"[^a-z0-9]+", " ", n)
    n = re.sub(r"\b(ltd|limited|co|company|general trading|store|duka|shop|supermarket)\b", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

def normalize_address(addr: str) -> str:
    if not addr: return ""
    a = addr.lower().strip()
    a = re.sub(r"[^a-z0-9]+", " ", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a

def normalize_website(url: str) -> str:
    if not url: return ""
    try:
        p = urlparse(url)
        net = p.netloc.lower().replace("www.", "")
        return urlunparse((p.scheme.lower(), net, p.path.rstrip('/'), '', '', ''))
    except Exception:
        return url

def make_dedupe_key(name, address, phone, website) -> str:
    base = f"{normalize_name(name)}|{normalize_address(address)}|{(phone or '').strip()}|{normalize_website(website or '')}"
    return hashlib.sha1(base.encode('utf-8')).hexdigest()

def safe_find_txt(driver, by, sel):
    try:
        return driver.find_element(by, sel).text.strip()
    except Exception:
        return ""

def save_record(rec: dict):
    try:
        dedupe_key = make_dedupe_key(rec.get('name'), rec.get('address'), rec.get('phone'), rec.get('website'))
        rec['norm_name'] = normalize_name(rec.get('name'))
        rec['norm_address'] = normalize_address(rec.get('address'))
        rec['dedupe_key'] = dedupe_key
        cur.execute(
            """
            INSERT INTO businesses
            (name, category, rating, address, phone, website, email, search_term, latitude, longitude, region, review_summary, review_sentiment, ts, norm_name, norm_address, dedupe_key)
            VALUES (:name, :category, :rating, :address, :phone, :website, :email, :search_term, :latitude, :longitude, :region, :review_summary, :review_sentiment, :ts, :norm_name, :norm_address, :dedupe_key)
            ON CONFLICT(dedupe_key) DO UPDATE SET
                category = COALESCE(excluded.category, category),
                rating = COALESCE(excluded.rating, rating),
                phone = COALESCE(excluded.phone, phone),
                website = COALESCE(excluded.website, website),
                email = COALESCE(excluded.email, email),
                region = COALESCE(excluded.region, region),
                review_summary = COALESCE(excluded.review_summary, review_summary),
                review_sentiment = COALESCE(excluded.review_sentiment, review_sentiment),
                ts = excluded.ts
            """,
            rec,
        )
        conn.commit()
    except Exception as e:
        logger.error(f"DB save error: {e}")

def update_checkpoint(term_idx, coord_idx, lat, lng):
    ts = datetime.datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT INTO checkpoints (id, term_idx, lat, lng, coord_idx, processed, updated_at)
        VALUES (1, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(id) DO UPDATE SET term_idx=excluded.term_idx, lat=excluded.lat, lng=excluded.lng, coord_idx=excluded.coord_idx, updated_at=excluded.updated_at
        """,
        (term_idx, lat, lng, coord_idx, ts),
    )
    conn.commit()

def load_checkpoint():
    cur.execute("SELECT term_idx, coord_idx FROM checkpoints WHERE id=1")
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return 0, 0

# -----------------------
# Core scraping
# -----------------------
def scrape_term_at_coord(driver, actions, term: str, lat: float, lng: float):
    global click_counter, proxy_use_count
    base_url = f"https://www.google.com/maps/search/{requests.utils.quote(term)}/@{lat},{lng},14z"
    driver.get(base_url)
    time.sleep(random.uniform(*ACTION_PAUSE))

    # block detection
    page_txt = driver.page_source.lower()
    if any(x in page_txt for x in ["unusual traffic", "verify you are a human", "systems have detected"]):
        live_progress("Block detected: rotating proxy & cooling down")
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(random.randint(120, 240))
        launch_browser(force_new_proxy=True)
        return 0

    # Locate results list panel (Google frequently changes class names)
    try:
        panel = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
        )
    except TimeoutException:
        try:
            panel = driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
        except NoSuchElementException:
            live_progress("Results panel not found; skipping coord")
            return 0

    seen_names = set()
    scraped_here = 0

    for s in range(SCROLL_LIMIT):
        time.sleep(random.uniform(*ACTION_PAUSE))
        cards = panel.find_elements(By.CSS_SELECTOR, 'div.Nv2PK')
        if not cards:
            break
        for c in cards:
            try:
                title_el = c.find_element(By.CSS_SELECTOR, 'a.hfpxzc')
                name = title_el.text.strip()
                if not name or name in seen_names:
                    continue
                seen_names.add(name)

                # click the card to open details panel
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", title_el)
                actions.move_to_element(title_el).pause(random.uniform(0.3, 0.8)).click().perform()
                click_counter += 1
                proxy_use_count += 1
                time.sleep(random.uniform(*ACTION_PAUSE) + 2)

                category = safe_find_txt(driver, By.CLASS_NAME, 'DkEaL')
                rating = safe_find_txt(driver, By.CLASS_NAME, 'F7nice')
                address = safe_find_txt(driver, By.CSS_SELECTOR, 'button[data-item-id="address"] div.Io6YTe') or safe_find_txt(driver, By.CSS_SELECTOR, 'div.Io6YTe')

                # phone, website buttons
                phone = safe_find_txt(driver, By.CSS_SELECTOR, 'button[aria-label^="Phone"] div.Io6YTe')
                website = ""
                try:
                    website = driver.find_element(By.CSS_SELECTOR, 'a[aria-label^="Website"]').get_attribute('href')
                except Exception:
                    pass

                # email from page source (rare)
                email = extract_email(driver.page_source)

                # review snippet
                rev = extract_google_reviews(driver.page_source)

                # region tag from coord center
                try:
                    region = geolocator.reverse(f"{lat}, {lng}", language='en').raw['address'].get('state')
                except Exception:
                    region = None

                rec = {
                    'name': name,
                    'category': category,
                    'rating': rating,
                    'address': address,
                    'phone': phone,
                    'website': website,
                    'email': email,
                    'search_term': term,
                    'latitude': lat,
                    'longitude': lng,
                    'region': region,
                    'review_summary': rev.get('summary'),
                    'review_sentiment': rev.get('sentiment'),
                    'ts': datetime.datetime.utcnow().isoformat()
                }
                save_record(rec)
                scraped_here += 1

                # adaptive throttle
                if click_counter % 25 == 0:
                    time.sleep(random.randint(10, 25))

                # relaunch periodically
                if RELAUNCH_EVERY and (click_counter % RELAUNCH_EVERY == 0):
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    launch_browser(force_new_proxy=True)
                    return scraped_here

                if scraped_here >= MAX_PER_COORD:
                    return scraped_here

            except Exception as e:
                logger.debug(f"Card parse error: {e}")
                continue

        # scroll the list panel
        try:
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;', panel)
        except Exception:
            break

    return scraped_here

# -----------------------
# Exporters (CSV/JSON/XLSX by region or full)
# -----------------------
def export_by_region(formats=EXPORT_FORMATS):
    os.makedirs(EXPORT_DIR, exist_ok=True)
    df = pd.read_sql_query("SELECT * FROM businesses", conn)
    if df.empty:
        live_progress("No data to export yet.")
        return
    ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    # disk space guard
    try:
        import shutil
        total, used, free = shutil.disk_usage(os.getcwd())
        if free < 500 * 1024 * 1024:
            live_progress("Low disk space; skipping export.")
            return
    except Exception:
        pass
    for region, g in df.groupby(df['region'].fillna('UNKNOWN')):
        safe_region = re.sub(r'[^A-Za-z0-9_-]+', '_', str(region))
        base = os.path.join(EXPORT_DIR, f"{safe_region}_{ts}")
        if "csv" in formats:
            g.to_csv(base + ".csv", index=False)
        if "json" in formats:
            g.to_json(base + ".json", orient="records", force_ascii=False)
        if "xlsx" in formats:
            try:
                g.to_excel(base + ".xlsx", index=False)
            except Exception as e:
                logger.warning(f"XLSX export failed for {region}: {e}")
    live_progress(f"Exported data by region to '{EXPORT_DIR}'.")

# -----------------------
# Orchestration
# -----------------------
def update_checkpoint_and_sleep(t_idx, c_idx, lat, lng):
    update_checkpoint(t_idx, c_idx, lat, lng)
    time.sleep(random.uniform(*COOLDOWN_BETWEEN_COORDS))

def db_backup():
    try:
        ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{DB_FILE}.{ts}.bak"
        with sqlite3.connect(backup_path) as b:
            conn.backup(b)
        live_progress(f"DB backup written: {backup_path}")
    except Exception as e:
        logger.warning(f"Backup failed: {e}")

def dedupe_soft():
    try:
        cur.execute(
            """
            DELETE FROM businesses
            WHERE id NOT IN (
                SELECT MAX(id) FROM businesses
                GROUP BY COALESCE(norm_name,''), COALESCE(norm_address,'')
            )
            AND (norm_name IS NOT NULL OR norm_address IS NOT NULL)
            """
        )
        conn.commit()
        live_progress("Soft dedupe pass complete")
    except Exception as e:
        logger.warning(f"Soft dedupe failed: {e}")

def db_maintenance():
    try:
        dedupe_soft()
        cur.execute("VACUUM")
        cur.execute("ANALYZE")
        conn.commit()
        live_progress("DB maintenance (DEDUP/VACUUM/ANALYZE) complete")
    except Exception as e:
        logger.warning(f"DB maintenance failed: {e}")

def run_scraper(max_hours: float | None = RUN_MAX_HOURS):
    start_time = time.time()
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR, exist_ok=True)

    # ensure dynamic terms exist
    if not os.path.exists(SEARCH_TYPES_FILE):
        build_dynamic_search_types()
    terms = load_search_types()

    start_term_idx, start_coord_idx = load_checkpoint()
    live_progress(f"Resume from term_idx={start_term_idx}, coord_idx={start_coord_idx}")

    driver, actions = launch_browser(force_new_proxy=True)

    try:
        for t_idx in range(start_term_idx, len(terms)):
            term = terms[t_idx]
            for c_idx in range(start_coord_idx if t_idx == start_term_idx else 0, len(COORDS)):
                if STOP_FLAG:
                    live_progress("Stop flag detected. Exiting run loop.")
                    return
                if max_hours and (time.time() - start_time) > max_hours * 3600:
                    live_progress("Max runtime reached. Stopping.")
                    return
                lat, lng = COORDS[c_idx]
                update_checkpoint(t_idx, c_idx, lat, lng)
                live_progress(f"Searching '{term}' @ {lat},{lng}")
                scraped = scrape_term_at_coord(driver, actions, term, lat, lng)
                live_progress(f"Scraped {scraped} records for '{term}' @ {lat},{lng}")
                update_checkpoint_and_sleep(t_idx, c_idx, lat, lng)
        # daily export and db maintenance at end of cycle
        export_by_region()
        db_maintenance()
    except KeyboardInterrupt:
        live_progress("Interrupted by user. Saving checkpoint...")
    except Exception as e:
        logger.error(f"Fatal loop error: {e}\n{traceback.format_exc()}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        conn.commit()
        live_progress("Run finished.")

# -----------------------
# FastAPI (local viewer + controls)
# -----------------------
app = FastAPI()

DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
<meta charset='utf-8'>
<title>TZ Biz Scraper Dashboard</title>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<style>
 body{font-family:Arial, sans-serif;max-width:1200px;margin:24px auto;padding:0 16px}
 header{display:flex;justify-content:space-between;align-items:center}
 .tabs{display:flex;gap:8px;margin:12px 0}
 .tab{padding:8px 12px;border:1px solid #222;border-radius:8px;cursor:pointer}
 .tab.active{background:#222;color:#fff}
 .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:16px 0}
 .card{border:1px solid #ddd;border-radius:10px;padding:14px}
 table{width:100%;border-collapse:collapse;margin-top:12px}
 th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;font-size:13px}
 .btn{padding:8px 12px;border-radius:8px;border:1px solid #222;background:#222;color:#fff;cursor:pointer}
 .btn.outline{background:#fff;color:#222}
 .muted{color:#666;font-size:12px}
 #chart{width:100%;height:260px;border:1px solid #eee;border-radius:8px}
</style>
</head>
<body>
<header>
  <h2>Tanzania Business Scraper</h2>
  <div>
    <button class='btn' onclick='refresh()'>Refresh</button>
    <button class='btn outline' onclick='exportNow()'>Export</button>
    <button class='btn outline' onclick='downloadDB()'>Download DB</button>
  </div>
</header>
<div class='tabs'>
  <div class='tab active' data-t='overview' onclick='showTab(this)'>Overview</div>
  <div class='tab' data-t='regions' onclick='showTab(this)'>Regions</div>
  <div class='tab' data-t='categories' onclick='showTab(this)'>Categories</div>
  <div class='tab' data-t='logs' onclick='showTab(this)'>Logs</div>
  <div class='tab' data-t='settings' onclick='showTab(this)'>Settings</div>
</div>
<section id='overview'>
  <div class='cards'>
    <div class='card'><div>Total Businesses</div><h3 id='total'>-</h3><div class='muted' id='lastRun'></div></div>
    <div class='card'><div>Search Terms</div><h3 id='terms'>-</h3></div>
    <div class='card'><div>Coord Cells</div><h3 id='coords'>-</h3></div>
    <div class='card'><div>Checkpoint</div><h3 id='checkpoint'>-</h3></div>
  </div>
  <canvas id='chart'></canvas>
</section>
<section id='regions' style='display:none'>
  <label>Filter: <input id='region' placeholder='Dar es Salaam' onkeyup='loadBusinesses()'></label>
  <table id='tblR'><thead><tr><th>Region</th><th>Count</th></tr></thead><tbody></tbody></table>
  <h4>Latest 50</h4>
  <table id='tblB'><thead><tr><th>Name</th><th>Category</th><th>Region</th><th>Rating</th><th>Phone</th></tr></thead><tbody></tbody></table>
</section>
<section id='categories' style='display:none'>
  <table id='tblC'><thead><tr><th>Category</th><th>Count</th></tr></thead><tbody></tbody></table>
</section>
<section id='logs' style='display:none'>
  <pre id='logbox' style='max-height:360px;overflow:auto;border:1px solid #eee;padding:8px;border-radius:8px'></pre>
</section>
<section id='settings' style='display:none'>
  <button class='btn' onclick='exportNow()'>Export Now</button>
  <button class='btn outline' onclick='backupNow()'>Backup DB</button>
</section>
<script>
let active='overview';
function showTab(el){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  active=el.dataset.t;
  document.querySelectorAll('section').forEach(s=>s.style.display='none');
  document.getElementById(active).style.display='block';
  refresh();
}
async function refresh(){
  const s = await fetch('/stats').then(r=>r.json());
  document.getElementById('total').textContent = s.total;
  document.getElementById('terms').textContent = s.terms;
  document.getElementById('coords').textContent = s.coords;
  document.getElementById('lastRun').textContent = 'UTC '+(s.time||'');
  const cp = await fetch('/progress').then(r=>r.json());
  document.getElementById('checkpoint').textContent = `term ${cp.term_idx} / coord ${cp.coord_idx}`;
  drawChart();
  if(active==='regions'){ loadRegions(); loadBusinesses(); }
  if(active==='categories'){ loadCategories(); }
  if(active==='logs'){ loadLogs(); }
}
async function loadBusinesses(){
  const region = document.getElementById('region').value;
  const q = region? `/businesses?limit=50&region=${encodeURIComponent(region)}` : '/businesses?limit=50';
  const rows = await fetch(q).then(r=>r.json());
  const tb = document.querySelector('#tblB tbody');
  tb.innerHTML = '';
  rows.forEach(r=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.name||''}</td><td>${r.category||''}</td><td>${r.region||''}</td><td>${r.rating||''}</td><td>${r.phone||''}</td>`;
    tb.appendChild(tr);
  })
}
async function loadRegions(){
  const rows = await fetch('/regions').then(r=>r.json());
  const tb = document.querySelector('#tblR tbody'); tb.innerHTML='';
  rows.forEach(r=>{ const tr=document.createElement('tr'); tr.innerHTML=`<td>${r.region||'UNKNOWN'}</td><td>${r.cnt}</td>`; tb.appendChild(tr); })
}
async function loadCategories(){
  const rows = await fetch('/categories').then(r=>r.json());
  const tb = document.querySelector('#tblC tbody'); tb.innerHTML='';
  rows.forEach(r=>{ const tr=document.createElement('tr'); tr.innerHTML=`<td>${r.category||'UNKNOWN'}</td><td>${r.cnt}</td>`; tb.appendChild(tr); })
}
async function loadLogs(){
  const t = await fetch('/logs').then(r=>r.text());
  document.getElementById('logbox').textContent = t;
}
async function exportNow(){ await fetch('/export', {method:'POST'}); alert('Exported to /exports'); }
async function backupNow(){ await fetch('/backup', {method:'POST'}); alert('DB backup created'); }
function downloadDB(){ window.location='/download/db'; }
// tiny chart: daily count line
async function drawChart(){
  const data = await fetch('/daily').then(r=>r.json());
  const c=document.getElementById('chart'); const ctx=c.getContext('2d');
  c.width=c.clientWidth; c.height=c.clientHeight; ctx.clearRect(0,0,c.width,c.height);
  if(!data.length){ ctx.fillText('No data yet',10,20); return; }
  const xs = data.map(d=>d.day); const ys=data.map(d=>d.cnt);
  const max=Math.max(...ys), min=Math.min(...ys);
  const pad=30; const W=c.width-pad*2; const H=c.height-pad*2;
  function sx(i){ return pad + (i/(xs.length-1))*W; }
  function sy(v){ return pad + H - ((v-min)/(max-min||1))*H; }
  ctx.strokeStyle='#333'; ctx.beginPath(); ctx.moveTo(sx(0), sy(ys[0]));
  for(let i=1;i<ys.length;i++){ ctx.lineTo(sx(i), sy(ys[i])); }
  ctx.stroke();
  ctx.fillStyle='#666'; ctx.fillText(xs[0], pad, c.height-5); ctx.fillText(xs[xs.length-1], c.width-60, c.height-5);
}
refresh(); setInterval(refresh, 10000);
</script>
</body>
</html>
"""

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.datetime.utcnow().isoformat()}

@app.get("/")
def root():
    return HTMLResponse(DASHBOARD_HTML)

@app.get("/stats")
def stats():
    cur.execute("SELECT COUNT(*) FROM businesses")
    total = cur.fetchone()[0]
    return {"total": total, "terms": len(load_search_types()), "coords": len(COORDS), "time": datetime.datetime.utcnow().isoformat()}

@app.get("/progress")
def progress():
    cur.execute("SELECT term_idx, coord_idx, updated_at FROM checkpoints WHERE id=1")
    row = cur.fetchone() or (0,0,None)
    return {"term_idx": row[0], "coord_idx": row[1], "updated_at": row[2]}

@app.get("/regions")
def regions():
    df = pd.read_sql_query("SELECT COALESCE(region,'UNKNOWN') AS region, COUNT(*) AS cnt FROM businesses GROUP BY region ORDER BY cnt DESC", conn)
    return json.loads(df.to_json(orient="records"))

@app.get("/categories")
def categories():
    df = pd.read_sql_query("SELECT COALESCE(category,'UNKNOWN') AS category, COUNT(*) AS cnt FROM businesses GROUP BY category ORDER BY cnt DESC LIMIT 50", conn)
    return json.loads(df.to_json(orient="records"))

@app.get("/daily")
def daily():
    df = pd.read_sql_query("SELECT substr(ts,1,10) AS day, COUNT(*) AS cnt FROM businesses GROUP BY substr(ts,1,10) ORDER BY day", conn)
    return json.loads(df.to_json(orient="records"))

@app.get("/logs")
def get_logs():
    try:
        with open(SCRAPE_LOG, 'r', encoding='utf-8') as f:
            return Response(f.read()[-20000:], media_type='text/plain')
    except Exception:
        return Response("", media_type='text/plain')

@app.get("/download/db")
def download_db():
    return FileResponse(DB_FILE, filename=Path(DB_FILE).name)

@app.post("/backup")
def backup_api():
    db_backup()
    return {"ok": True}

@app.get("/businesses")
def get_businesses(limit: int = 100, offset: int = 0, region: str | None = None):
    query = "SELECT * FROM businesses"
    params = []
    if region:
        query += " WHERE region = ?"
        params.append(region)
    query += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    df = pd.read_sql_query(query, conn, params=params)
    return json.loads(df.to_json(orient="records"))

@app.post("/export")
def export_api():
    export_by_region()
    return {"exported": True}

def run_api():
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")

# -----------------------
# Scheduler & CLI Entrypoint
# -----------------------
def handle_sigterm(signum, frame):
    global STOP_FLAG
    STOP_FLAG = True
    live_progress("Received stop signal. Finishing current cycle…")

signal.signal(signal.SIGINT, handle_sigterm)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, handle_sigterm)

def launch_gui():
    # Minimal GUI for double-click run
    def start_now():
        threading.Thread(target=run_scraper, kwargs={"max_hours": RUN_MAX_HOURS}, daemon=True).start()
        live_progress("Started immediate scraping run from GUI.")
    def open_ui():
        webbrowser.open("http://127.0.0.1:8000/")
    def do_export():
        export_by_region()
    def quit_app():
        os._exit(0)

    # ensure API running
    th = threading.Thread(target=run_api, daemon=True); th.start()

    win = tk.Tk(); win.title("Tanzania Business Scraper")
    win.geometry("420x260")
    tk.Label(win, text="Tanzania Business Scraper", font=("Segoe UI", 14, "bold")).pack(pady=10)
    tk.Button(win, text="Run Now", width=22, command=start_now).pack(pady=6)
    tk.Button(win, text="Open Dashboard", width=22, command=open_ui).pack(pady=6)
    tk.Button(win, text="Export by Region", width=22, command=do_export).pack(pady=6)
    tk.Label(win, text=f"Scheduled daily at {RUN_AT}").pack(pady=6)
    tk.Button(win, text="Exit", width=22, command=quit_app).pack(pady=6)
    win.mainloop()

def main():
    global COORD_GRID_STEP, HEADLESS, CHROMEDRIVER_PATH, PROXY_LIST_FILE, RUN_AT
    global TERM_FILTER, REGION_FILTER, EXPORT_FORMATS, RUN_MAX_HOURS, ROTATE_PROXY_EVERY

    # If launched by double-click (no args), default to GUI mode
    if len(sys.argv) == 1:
        launch_gui()
        return

    parser = argparse.ArgumentParser(description="Tanzania Maps Scraper")
    parser.add_argument("--run-now", action="store_true", help="Run scraping immediately")
    parser.add_argument("--headless", type=int, default=1, help="1=headless, 0=visible")
    parser.add_argument("--grid-step", type=float, default=COORD_GRID_STEP, help="Grid step in degrees")
    parser.add_argument("--relaunch-every", type=int, default=RELAUNCH_EVERY, help="Clicks before relaunch")
    parser.add_argument("--rotate-proxy-every", type=int, default=ROTATE_PROXY_EVERY, help="Coords before proxy rotate")
    parser.add_argument("--driver", type=str, default=None, help="Path to chromedriver.exe")
    parser.add_argument("--proxies", type=str, default=None, help="Path to proxies.txt")
    parser.add_argument("--term-filter", type=str, default=None, help="Only terms containing this substring")
    parser.add_argument("--run-at", type=str, default=RUN_AT, help="Daily start HH:MM")
    parser.add_argument("--max-hours", type=float, default=None, help="Auto-stop after N hours")
    parser.add_argument("--export-now", action="store_true", help="Export by region now and exit")
    parser.add_argument("--formats", type=str, default=",".join(EXPORT_FORMATS), help="Export formats csv,json,xlsx")

    args = parser.parse_args()

    HEADLESS = bool(int(args.headless))
    COORD_GRID_STEP = args.grid_step
    RELAUNCH_EVERY = args.relaunch_every
    ROTATE_PROXY_EVERY = args.rotate_proxy_every
    CHROMEDRIVER_PATH = args.driver or CHROMEDRIVER_PATH
    PROXY_LIST_FILE = args.proxies or PROXY_LIST_FILE
    RUN_AT = args.run_at
    TERM_FILTER = args.term_filter
    RUN_MAX_HOURS = args.max_hours
    EXPORT_FORMATS = [f.strip() for f in args.formats.split(',') if f.strip()]

    # Regenerate coords if step changed
    global COORDS
    COORDS = generate_coords(COORD_GRID_STEP)

    # Reload proxies if provided
    if PROXY_LIST_FILE:
        globals()['PROXIES'] = load_proxies()

    if args.export_now:
        export_by_region(EXPORT_FORMATS)
        return

    # ensure dynamic terms exist
    if not os.path.exists(SEARCH_TYPES_FILE):
        build_dynamic_search_types()

    # schedule daily run
    schedule.clear()
    schedule.every().day.at(RUN_AT).do(run_scraper, RUN_MAX_HOURS)
    live_progress(f"Scheduler active. First run at {RUN_AT}. Use --run-now to start immediately.")

    # start API in a background thread
    th = threading.Thread(target=run_api, daemon=True)
    th.start()

    # optional immediate run
    if args.run_now:
        run_scraper(RUN_MAX_HOURS)

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
