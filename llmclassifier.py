# =============================================================
# Samsung Members Scraper (7 Markets) — Posts + Replies (Robust, Parallel)
# Markets supported: SEIN (Indonesia), SEAU (Australia), TSE (Thailand),
#                    SME (Malaysia), SESP (Singapore), SEPCO (Philippines), SENZ (New Zealand)
# =============================================================

import os, re, time, math
import pandas as pd
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------
# SETTINGS (EDIT THESE)
# ---------------------------
MARKET = "SEIN"          # SEIN / SEAU / TSE / SME / SESP / SEPCO / SENZ
START_PAGE = 51          # for single page, set START_PAGE = STOP_PAGE
STOP_PAGE  = 55
HEADLESS   = True
N_WORKERS  = 4

# Optional: keep AuthorRaw for QA
KEEP_AUTHOR_RAW = True

# ---------------------------
# MARKET CONFIG
# ---------------------------
BASE = "https://r1.community.samsung.com"

MARKETS = {
    # Indonesia
    "SEIN": {
        "sub_code": "SEIN",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/community/ct-p/id-community?page={p}&tab=recent_topics",
            f"{BASE}/t5/community/bd-p/id-community?page={p}&tab=recent_topics",
        ],
    },
    # Australia
    "SEAU": {
        "sub_code": "SEAU",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/community/ct-p/au-community?page={p}&tab=recent_topics",
            f"{BASE}/t5/community/bd-p/au-community?page={p}&tab=recent_topics",
        ],
    },
    # Thailand
    "TSE": {
        "sub_code": "TSE",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/community/ct-p/th-community?page={p}&tab=recent_topics",
            f"{BASE}/t5/community/bd-p/th-community?page={p}&tab=recent_topics",
        ],
    },
    # Malaysia
    "SME": {
        "sub_code": "SME",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/community/ct-p/my-community?page={p}&tab=recent_topics",
            f"{BASE}/t5/community/bd-p/my-community?page={p}&tab=recent_topics",
        ],
    },
    # Singapore
    "SESP": {
        "sub_code": "SESP",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/singapore/ct-p/sg?profile.language=en&page={p}&tab=recent_topics",
            f"{BASE}/t5/singapore/bd-p/sg?profile.language=en&page={p}&tab=recent_topics",
        ],
    },
    # Philippines
    "SEPCO": {
        "sub_code": "SEPCO",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/community/ct-p/ph-community?page={p}&tab=recent_topics",
            f"{BASE}/t5/community/bd-p/ph-community?page={p}&tab=recent_topics",
        ],
    },
    # New Zealand
    "SENZ": {
        "sub_code": "SENZ",
        "listing_candidates": lambda p: [
            f"{BASE}/t5/new-zealand/ct-p/nz?profile.language=en&tab=recent_topics&page={p}",
            f"{BASE}/t5/new-zealand/bd-p/nz?profile.language=en&tab=recent_topics&page={p}",
        ],
    },
}

if MARKET not in MARKETS:
    raise ValueError(f"Unsupported MARKET={MARKET}. Choose from: {', '.join(MARKETS)}")

SUB_CODE = MARKETS[MARKET]["sub_code"]

# ---------------------------
# Desktop path (C:/D:/ + OneDrive)
# ---------------------------
def get_desktop_path() -> str:
    candidates = []
    home = os.path.expanduser("~")
    candidates.append(os.path.join(home, "Desktop"))

    one_drive = os.environ.get("OneDrive") or os.environ.get("ONEDRIVE")
    if one_drive:
        candidates.append(os.path.join(one_drive, "Desktop"))

    for base in list(candidates):
        if isinstance(base, str) and base.startswith("C:"):
            candidates.append(base.replace("C:", "D:", 1))

    for p in candidates:
        if p and os.path.isdir(p):
            return p

    fallback = os.path.join(home, "Desktop")
    os.makedirs(fallback, exist_ok=True)
    return fallback

desktop = get_desktop_path()
if START_PAGE == STOP_PAGE:
    OUTFILENAME = f"samsung_members_{MARKET.lower()}_page{START_PAGE:02}.xlsx"
else:
    OUTFILENAME = f"samsung_members_{MARKET.lower()}_page{START_PAGE:02}to{STOP_PAGE:02}.xlsx"
OUTFILE = os.path.join(desktop, OUTFILENAME)

# ---------------------------
# Chrome setup (robust + eager)
# ---------------------------
def configure_chrome_options(headless: bool = True) -> Options:
    opts = Options()

    # Robust binary detection (C/D + x86/x64)
    for path in [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"D:\Program Files\Google\Chrome\Application\chrome.exe",
        r"D:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]:
        if os.path.isfile(path):
            opts.binary_location = path
            break

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,1800")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })

    # Faster page return
    opts.page_load_strategy = "eager"
    return opts

# Primary listing driver
options = configure_chrome_options(HEADLESS)
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
WAIT = WebDriverWait(driver, 20)

# Reuse chromedriver path for workers
CHROMEDRIVER_PATH = service.path

# ---------------------------
# Helpers
# ---------------------------
def normalize_url(href: str) -> str:
    if not href:
        return ""
    url = urljoin(BASE, str(href).strip())
    # fix accidental double base
    url = re.sub(r"^https://r1\.community\.samsung\.comhttps://", "https://", url)
    return url

def accept_cookies_if_present(_driver):
    try:
        WebDriverWait(_driver, 5).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "#onetrust-accept-btn-handler, button[aria-label*='Accept']")
            )
        ).click()
        time.sleep(0.2)
    except Exception:
        pass

def force_full_timestamps(_driver):
    """
    Copy abbr[title] timestamp into visible text when present.
    Helps markets that show relative labels in UI.
    """
    try:
        _driver.execute_script("""
            document.querySelectorAll('div.author abbr[title]').forEach(el=>{
                const t = el.getAttribute('title');
                if (t) el.innerText = t;
            });
        """)
    except Exception:
        pass

def wait_for_tiles_or_retry(urls_for_page):
    """
    Try multiple listing URLs (ct-p / bd-p) until tiles are detected.
    Returns (tile_selector, landed_url)
    """
    def load_and_detect(url):
        driver.get(url)
        accept_cookies_if_present(driver)
        force_full_timestamps(driver)

        for _ in range(2):  # short scrolls only
            driver.execute_script("window.scrollBy(0, 900)")
            time.sleep(0.2)

        selectors = [
            "article.samsung-message-tile",
            "li.samsung-message-tile",
            "div.samsung-message-tile",
            ".lia-message-list .samsung-message-tile",
        ]

        def any_tiles(drv):
            for sel in selectors:
                if drv.find_elements(By.CSS_SELECTOR, sel):
                    return sel
            return False

        sel = WebDriverWait(driver, 12).until(lambda d: any_tiles(d))
        return sel, url

    last_exc = None
    for u in urls_for_page:
        try:
            return load_and_detect(u)
        except Exception as e:
            last_exc = e

    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to load listing page.")

NBSP = "\u00A0"
def clean_snippet(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    txt = text.replace(NBSP, " ")
    txt = re.sub(
        r"View\s*Post\s*\d+\s*Views\s*\d+\s*Replies\s*\d+\s*Like(?:s)?",
        "",
        txt,
        flags=re.IGNORECASE,
    )
    txt = re.sub(r"View\s*Post[\s\S]*?Like(?:s)?", "", txt, flags=re.IGNORECASE)
    return " ".join(txt.split()).strip()

# Legacy fallback parser (text split)
def parse_author_field(author_raw: str):
    lines = [ln.strip() for ln in str(author_raw).split("\n") if ln.strip()]
    author = date_part = time_part = category = ""
    if lines:
        author = lines[0]
        for ln in lines:
            m = re.search(r"(\d{2}-\d{2}-\d{4})\s+(\d{2}:\d{2}\s+(?:AM|PM))", ln, flags=re.I)
            if m:
                date_part, time_part = m.group(1), m.group(2)
                break
            m2 = re.search(r"(\d{2}-\d{2}-\d{4})", ln)
            if m2 and not date_part:
                date_part = m2.group(1)
        category = lines[-1] if len(lines) > 1 else ""
    return pd.Series([author, date_part, time_part, category])

def extract_author_meta_from_tile(tile):
    """
    DOM-first extractor:
      - AuthorRaw = div.author innerText
      - AuthorName: a.login -> a.lia-user-name-link -> a.username -> a[rel='author'] -> any user-profile link
      - Date/Time: abbr[title] or <time>, regex fallback on AuthorRaw
      - Category: last non-user <a> in div.author
    Returns: (author_name, date_part, time_part, category, author_raw)
    """
    author_name = date_part = time_part = category = ""
    author_raw = ""

    try:
        ablock = tile.find_element(By.CSS_SELECTOR, "div.author")
        author_raw = (ablock.text or "").strip()

        # 1) AuthorName (DOM-first)
        author_selectors = [
            "a.login",
            "a.lia-user-name-link",
            "a.username",
            "a[rel='author']",
            "a[href*='/t5/user/']",
            "a[href*='/user/viewprofilepage']",
        ]
        for sel in author_selectors:
            try:
                for el in ablock.find_elements(By.CSS_SELECTOR, sel):
                    txt = (el.text or "").strip()
                    if txt:
                        author_name = txt
                        raise StopIteration
            except StopIteration:
                break
            except Exception:
                pass

        # Fallback to first non-empty line of AuthorRaw if still missing
        if not author_name and author_raw:
            lines = [x.strip() for x in author_raw.split("\n") if x.strip()]
            if lines:
                author_name = lines[0]

        # 2) Date / Time (abbr[title] or <time>)
        stamp = ""
        try:
            t_candidates = ablock.find_elements(By.CSS_SELECTOR, "abbr[title], time")
            for t in t_candidates:
                stamp = (t.get_attribute("title") or t.text or "").strip()
                if stamp:
                    break
        except Exception:
            pass

        # Fallback parse from raw text
        if not stamp:
            stamp = author_raw

        m = re.search(r"(\d{2}-\d{2}-\d{4})\s+(\d{2}:\d{2}\s+(?:AM|PM))", stamp, flags=re.I)
        if m:
            date_part, time_part = m.group(1), m.group(2)
        else:
            # if only absolute date exists
            m2 = re.search(r"(\d{2}-\d{2}-\d{4})", stamp)
            if m2:
                date_part = m2.group(1)
            # relative times ("6m ago", "3h ago", "2 days ago") intentionally left blank

        # 3) Category = last non-user link in div.author
        try:
            links = ablock.find_elements(By.CSS_SELECTOR, "a")
            for link in reversed(links):
                txt = (link.text or "").strip()
                if not txt:
                    continue
                cls = (link.get_attribute("class") or "")
                href = (link.get_attribute("href") or "")

                # Skip user/profile links
                if ("login" in cls) or ("UserAvatar" in cls):
                    continue
                if ("/t5/user/" in href) or ("/user/viewprofilepage" in href):
                    continue

                category = txt
                break
        except Exception:
            pass

    except Exception:
        pass

    return author_name, date_part, time_part, category, author_raw

def month_from_date(date_str: str) -> str:
    """
    Expects MM-dd-yyyy. If missing/relative => Unknown.
    No inference across month boundaries.
    """
    try:
        m = int(str(date_str).split("-")[0])
        months = ["Unknown","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        return months[m] if 1 <= m <= 12 else "Unknown"
    except Exception:
        return "Unknown"

# ---------------------------
# Detail page fetch (single-driver function)
# ---------------------------
def fetch_post_and_replies_with_driver(drv, url: str):
    """
    Returns (full_post_text, replies_text_concat, replies_count)
    """
    try:
        drv.get(url)
        accept_cookies_if_present(drv)

        WebDriverWait(drv, 8).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR, "#bodyDisplay, #messageView2, .lia-message-view-wrapper"
            ))
        )

        # Expand truncation if present (2x max for speed)
        for _ in range(2):
            try:
                more = drv.find_element(
                    By.CSS_SELECTOR,
                    "a.lia-message-read-more, a.lia-truncate-read-more, button[aria-controls*='truncate']"
                )
                drv.execute_script("arguments[0].click();", more)
                time.sleep(0.15)
            except Exception:
                break

        # Main post body (first body-content block)
        post_blocks = drv.find_elements(
            By.CSS_SELECTOR,
            "#bodyDisplay .lia-message-body-content, "
            "#messageView2 .lia-message-body-content, "
            ".lia-message-view-wrapper .lia-message-body-content"
        )
        main_txt = ""
        if post_blocks:
            t = (post_blocks[0].get_attribute("innerText") or "").strip()
            if t:
                main_txt = "\n".join(ln.strip() for ln in t.splitlines() if ln.strip())

        # Replies (exclude first message)
        reply_blocks = drv.find_elements(
            By.CSS_SELECTOR,
            ".linear-message-list .lia-message-view:not(.first-message) .lia-message-body-content, "
            ".custom-reply .lia-message-body-content"
        )
        replies = []
        for r in reply_blocks:
            t = (r.get_attribute("innerText") or "").strip()
            if t:
                replies.append(t)

        return main_txt, " || ".join(replies), len(replies)

    except Exception:
        # Defensive fallback
        try:
            alt = drv.find_element(By.CSS_SELECTOR, ".lia-quilt-column-main-content, .lia-quilt-row-main")
            t = (alt.get_attribute("innerText") or "").strip()
            t = "\n".join(ln.strip() for ln in t.splitlines() if ln.strip())
            return t[:20000], "", 0
        except Exception:
            return "", "", 0

# ---------------------------
# Worker driver (parallel)
# ---------------------------
def new_worker_driver():
    opts = configure_chrome_options(True)  # workers always headless
    d = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=opts)

    # Block heavy resources (best-effort)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setBlockedURLs", {
            "urls": [
                "*.png","*.jpg","*.jpeg","*.gif","*.webp","*.svg",
                "*.woff","*.woff2","*.ttf","*.otf",
                "*.mp4","*.webm","*.avi","*.mkv",
                "*.css"  # optional, but faster
            ]
        })
    except Exception:
        pass

    d.set_page_load_timeout(15)
    return d

def worker(urls_chunk):
    drv = new_worker_driver()
    out = {}
    try:
        for u in urls_chunk:
            out[u] = fetch_post_and_replies_with_driver(drv, u)
    finally:
        try:
            drv.quit()
        except Exception:
            pass
    return out

# ---------------------------
# MAIN — 1) Crawl listing pages and collect unique URLs
# ---------------------------
all_rows = []
seen_urls = set()

t0 = time.perf_counter()

for page in range(START_PAGE, STOP_PAGE + 1):
    page_urls = MARKETS[MARKET]["listing_candidates"](page)
    print(f"\n=== {MARKET} Listing page {page} ===")

    try:
        tile_selector, landed = wait_for_tiles_or_retry(page_urls)
        print(f"✓ Landed: {landed} | selector: {tile_selector}")
    except Exception as e:
        print(f"× Could not load tiles for page {page}: {e}")
        continue

    tiles = driver.find_elements(By.CSS_SELECTOR, tile_selector)
    print(f"Found {len(tiles)} tiles on page {page}")

    for post in tiles:
        try:
            # Title + URL
            a = post.find_element(By.CSS_SELECTOR, "h3 a")
            title = (a.text or "").strip()
            href = normalize_url(a.get_attribute("href") or "")

            if not href or href in seen_urls:
                continue
            seen_urls.add(href)

            # Snippet
            try:
                snippet = (post.find_element(By.CSS_SELECTOR, "div.content-wrapper").text or "").strip()
            except Exception:
                snippet = ""

            # Author metadata (DOM-first + fallback)
            author_name, date_part, time_part, category, author_raw = extract_author_meta_from_tile(post)

            if (not author_name or not category) and author_raw:
                s = parse_author_field(author_raw)
                author_name = author_name or (s.iloc[0] if len(s) > 0 else "")
                date_part   = date_part   or (s.iloc[1] if len(s) > 1 else "")
                time_part   = time_part   or (s.iloc[2] if len(s) > 2 else "")
                category    = category    or (s.iloc[3] if len(s) > 3 else "")

            # Counts
            def get_int(css):
                try:
                    txt = (post.find_element(By.CSS_SELECTOR, css).text or "").strip()
                    txt = re.sub(r"[^\d]", "", txt)
                    return int(txt) if txt else 0
                except Exception:
                    return 0

            views    = get_int("li.samsung-tile-views b")
            comments = get_int("li.samsung-tile-replies b")
            likes    = get_int("li.samsung-tile-kudos b")

            row = {
                "Title": title,
                "URL": href,
                "AuthorName": author_name,
                "Date": date_part,
                "Time": time_part,
                "Category": category,
                "Likes": likes,
                "Comments": comments,
                "Views": views,
                "Snippet": snippet,
                "ListingPage": page,
            }
            if KEEP_AUTHOR_RAW:
                row["AuthorRaw"] = author_raw

            all_rows.append(row)

        except Exception as e:
            print("Tile parse error:", e)

# Build dataframe
df = pd.DataFrame(all_rows)

# ---------------------------
# 2) Transform / clean
# ---------------------------
if not df.empty:
    df["Month"] = df["Date"].apply(month_from_date)
    df["Sub"] = SUB_CODE
    df["Snippet"] = df["Snippet"].apply(clean_snippet)

# Listing timing
t1 = time.perf_counter()
print(f"\n⏱ Listing phase done in {t1 - t0:.1f}s | rows={len(df)}")

# ---------------------------
# 3) Fetch FULL post text + Replies (PARALLEL)
# ---------------------------
# Close listing driver before worker drivers spawn (reduces resource usage)
try:
    driver.quit()
except Exception:
    pass

full_texts, replies_texts, replies_counts = [], [], []

urls = df["URL"].tolist() if (not df.empty and "URL" in df.columns) else []
if urls:
    # Round-robin chunk split
    chunks = [urls[i::N_WORKERS] for i in range(N_WORKERS)]

    print(f"Starting detail fetch with {N_WORKERS} workers for {len(urls)} URLs...")
    t2 = time.perf_counter()

    results = {}
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        futures = [ex.submit(worker, chunk) for chunk in chunks if chunk]
        done_n = 0
        for fut in as_completed(futures):
            batch = fut.result()
            results.update(batch)
            done_n += len(batch)
            print(f"  ...detail progress: {done_n}/{len(urls)}")

    # Stitch back in original order
    for i, u in enumerate(urls, start=1):
        main_txt, rep_txt, rep_cnt = results.get(u, ("", "", 0))
        full_texts.append(main_txt)
        replies_texts.append(rep_txt)
        replies_counts.append(rep_cnt)

    t3 = time.perf_counter()
    print(f"⏱ Detail phase done in {t3 - t2:.1f}s")

    df["FullText"] = full_texts
    df["Replies"] = replies_texts
    df["RepliesCount"] = replies_counts

# ---------------------------
# 4) Save to Desktop
# ---------------------------
df.to_excel(OUTFILE, index=False)

t_end = time.perf_counter()
print(f"\n✅ Saved -> {OUTFILE}")
print(f"Rows: {len(df)} | Pages: {START_PAGE}..{STOP_PAGE} | Market: {MARKET}")
print(f"⏱ Total runtime: {t_end - t0:.1f}s")