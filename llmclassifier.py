# =========================
# Samsung Members + AI Classifier Pipeline
# =========================
# pip install -U openai pandas openpyxl
# Ensure OPENAI_API_KEY is set in environment.

from __future__ import annotations
import os, re, json, time, unicodedata
import pandas as pd
from openai import OpenAI

client = OpenAI()  # uses OPENAI_API_KEY

# ========= 1) CONFIG =========
MODEL = "gpt-4.1-mini"

# ======= Samsung Stars canon (hard-coded) =======
STAR_CANON = {
    "pntv1905","davidbui13","nguyennam","Jiyoon051","thaoxuka","Garam","SnehaTS","AmeetM","Jodsta",
    "HS_NzAu","Dolgogi","mbckl","VueeyLe","Manue12","Dietta","Vee33","shelley_","photosbyraffy",
    "Nessaslifestylediary","_nok_","djrules24","Yasmik","Cgers80","StephenMC","nxmah","duggle",
    "abdurraim","BagusPrisandhy","AriMantep","anggazone08","Henders-","krisnugroho","bagusjpg",
    "bayJoee","galaxyOD","KAKPJ","Uqiuqian","CaptainLynnnn","alinrizkiana","Alvitooo","mikaelrinto",
    "fazri91","รςøгקเѻภ_ฝคгร","Kbbbb","Jeremyeyeguy","JamieGems","peeonurhead","TheAseanPrince",
    "nattooh","SG_Yap","Wolfsbanee","Sukasblood","klausandfound","unclechan","23edl_","renwei89",
    "nazzzz","jun4hong2","Tiganamploh","YOLO_CY","theroytravels","aliasyraf","Keryin","jonloo1126",
    "Angpaologized","PPHATTARAPONG","ttoeytraveller","Bern2Hell","bagol1209","TimmyRose",
    "SummeRamirez","MarkLuceño","LiezlNierves"
}

def _norm_name(s: str) -> str:
    """Normalize usernames: lowercase, remove accents, trim punctuation, collapse spaces."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii", errors="ignore")
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = s.strip("@#-–—•*_|()[]{}:,;.!?\"'")
    return s

SAMSUNG_STARS = { _norm_name(x) for x in STAR_CANON if _norm_name(x) }

MODERATOR_PATTERNS = [
    re.compile(r"global_contents", re.I),
    re.compile(r"\bsamsung\b", re.I),
]

def classify_posted_by(author_name: str) -> str:
    if not author_name:
        return "Member"
    norm = _norm_name(author_name)
    if norm in SAMSUNG_STARS:
        return "Samsung Star"
    if any(p.search(author_name) for p in MODERATOR_PATTERNS):
        return "Moderator"
    return "Member"


# ======= Canon product list =======
CANON = [
    "Galaxy S20","Galaxy S21","Galaxy S22","Galaxy S23","Galaxy S24","Galaxy S25",
    "Galaxy Z Flip3","Galaxy Z Flip4","Galaxy Z Flip5","Galaxy Z Flip6","Galaxy Z Flip7",
    "Galaxy Z Fold2","Galaxy Z Fold3","Galaxy Z Fold4","Galaxy Z Fold5","Galaxy Z Fold6","Galaxy Z Fold7",
    "Galaxy Tab S6","Galaxy Tab S7","Galaxy Tab S8","Galaxy Tab S9","Galaxy Tab S10",
    "Galaxy Tab A7","Galaxy Tab A8","Galaxy Tab A9","Galaxy Tab A10",
    "Galaxy A01","Galaxy A02","Galaxy A03","Galaxy A04","Galaxy A05","Galaxy A06","Galaxy A10","Galaxy A11",
    "Galaxy A12","Galaxy A13","Galaxy A14","Galaxy A15","Galaxy A16","Galaxy A20","Galaxy A21","Galaxy A22","Galaxy A23",
    "Galaxy A24","Galaxy A25","Galaxy A26","Galaxy A2","Galaxy A30","Galaxy A31","Galaxy A32","Galaxy A33","Galaxy A34",
    "Galaxy A35","Galaxy A36","Galaxy A50","Galaxy A51","Galaxy A52","Galaxy A53","Galaxy A54","Galaxy A55","Galaxy A56",
    "Galaxy A70","Galaxy A71","Galaxy A72","Galaxy A73","Galaxy A7","Galaxy A80","Galaxy A9",
    "Galaxy M02","Galaxy M10","Galaxy M11","Galaxy M12","Galaxy M14","Galaxy M15","Galaxy M20","Galaxy M21","Galaxy M22",
    "Galaxy M23","Galaxy M30","Galaxy M31","Galaxy M32","Galaxy M33","Galaxy M34","Galaxy M51","Galaxy M52","Galaxy M53","Galaxy M54","Galaxy M62",
    "Galaxy Watch 3","Galaxy Watch 4","Galaxy Watch 5","Galaxy Watch 6","Galaxy Watch 7","Galaxy Watch 8","Galaxy Watch FE","Galaxy Watch Ultra","Galaxy Watch Active",
    "Galaxy Buds","Galaxy Buds 2","Galaxy Buds 3","Galaxy Buds FE","Galaxy Buds Live","Galaxy Buds Plus","Galaxy Buds Pro",
    "Monitor","Soundbar","Refrigerator","Laundry","Air Conditioner","Vacuum Cleaner","Microwave",
    "The Frame","The Serif","The Sero","The Premiere","The Freestyle","OLED","NEO QLED","QLED","Crystal UHD"
]

def build_category_map(products):
    m = {}
    for p in products:
        low = p.lower()
        if "z flip" in low:
            m[p] = "Galaxy Z Flip"
        elif "z fold" in low:
            m[p] = "Galaxy Z Fold"
        elif re.search(r"\bs\d{2}\b", low):
            m[p] = "Galaxy S"
        elif "tab s" in low:
            m[p] = "Galaxy Tab S"
        elif "tab a" in low:
            m[p] = "Galaxy Tab A"
        elif re.search(r"\ba\d{1,2}\b", low):
            m[p] = "Galaxy A"
        elif re.search(r"\bm\d{1,2}\b", low):
            m[p] = "Galaxy M"
        elif "watch" in low:
            m[p] = "Galaxy Watch"
        elif "buds" in low:
            m[p] = "Galaxy Buds"
        elif "monitor" in low:
            m[p] = "Monitor"
        elif "soundbar" in low:
            m[p] = "Soundbar"
        elif "refrigerator" in low:
            m[p] = "Refrigerator"
        elif "laundry" in low:
            m[p] = "Laundry"
        elif "microwave" in low:
            m[p] = "Microwave"
        elif "air conditioner" in low or "aircon" in low or "air-con" in low:
            m[p] = "Air Conditioner"
        elif "vacuum" in low:
            m[p] = "Vacuum Cleaner"
        else:
            m[p] = "Others"
    return m

CATEGORY_MAP = build_category_map(CANON)

def assign_category(product_name: str) -> str:
    if not product_name:
        return "Others"
    if product_name in CATEGORY_MAP:
        return CATEGORY_MAP[product_name]
    for known, cat in CATEGORY_MAP.items():
        if known.lower() in product_name.lower():
            return cat
    return "Others"


# ========= 2) AI Prompt (Subtopics + Topic mapping) =========
SUBTOPICS = [
  "Contest","Events","Information","Competitor","Agent","Promo",
  "Price / Purchase Inquiry","Recommendation","Shipping","Warranty","Accessories",
  "Software","Camera","Screen / Display","AI","Battery / Charging","Account",
  "Performance","Connectivity / Network","Audio / Calls","Storage / Memory","Design","Apps","Others"
]

PROMPT_GUIDE = f"""
You are an expert annotator of Samsung Members / Samsung Community forum posts.
Return ONLY a valid JSON object with key "items" = array of results.
Each array element MUST be an object with keys:
- i: integer index of the input line
- ss_product: most specific Samsung product model discussed (e.g. "Galaxy S24 Ultra"). If none, "No specific product".
- product_category: one of ["Galaxy S","Galaxy Z Flip","Galaxy Z Fold","Galaxy Tab S","Galaxy Tab A","Galaxy A","Galaxy M","Galaxy Watch","Galaxy Buds","Monitor","Soundbar","Refrigerator","Laundry","Air Conditioner","Vacuum Cleaner","Microwave","Others"]
- subtopic: choose EXACTLY one from: {SUBTOPICS}
- topic: choose based on subtopic (EXACT strings):
  * Contest -> "Contest"
  * Events, Information -> "News"
  * Competitor -> "Competitor"
  * Promo -> "Promo"
  * Price / Purchase Inquiry, Recommendation, Shipping -> "Purchase & Orders"
  * Agent -> "Service"
  * Warranty, Accessories, Software, Camera, Screen / Display, AI, Battery / Charging, Account,
    Performance, Connectivity / Network, Audio / Calls, Storage / Memory, Design, Apps
    -> "Product (Support)"
  * If the post is a review / test / general impressions and does NOT contain help-seeking cues
    ("how to","please help","need help","need advice","seek support","bug","fix","error code","is it possible")
    -> "Product (General)"
  * Otherwise -> "Others"
- sentiment: one of ["Positive","Negative","Neutral","Mix"]
  * Neutral = factual/informative/acknowledgment only
  * Mix = clear positive AND negative cues (esp. with connectors "but","however","though","although","yet","nevertheless","still")
- brand_terms: array of Samsung products/families/categories mentioned (deduplicate; keep specificity)

Guardrails:
1) Decide subtopic first, then topic from mapping.
2) Be as specific as possible for ss_product; do not output generic "Smartphone" if a model can be inferred.
3) brand_terms should include ALL relevant Samsung product mentions, not only the main one.
4) Output JSON only.
""".strip()


# ========= 3) Classify a batch in JSON mode =========
def _clip(s, max_len=1000):
    s = "" if s is None else str(s)
    return s if len(s) <= max_len else s[:max_len]

def classify_batch_json_mode_ai(texts, model=MODEL, sleep=0.3):
    numbered = "\n\n".join([f"[{i}] {_clip(t)}" for i, t in enumerate(texts)])
    chat = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": PROMPT_GUIDE},
            {"role": "user", "content": f"Classify these lines and return JSON with key 'items'. Align using 'i':\n\n{numbered}"},
        ],
        response_format={"type": "json_object"},
        temperature=0
    )

    items = []
    try:
        data = json.loads(chat.choices[0].message.content)
        items = data.get("items", [])
    except Exception:
        items = []

    by_i = {}
    for it in items:
        try:
            i = int(it.get("i"))
        except Exception:
            continue

        ss_prod = str(it.get("ss_product", "")).strip()
        cat_ai  = str(it.get("product_category", "")).strip()
        cat_det = assign_category(ss_prod) if (ss_prod and ss_prod != "No specific product") else (cat_ai or "Others")

        brand_terms = it.get("brand_terms", [])
        if not isinstance(brand_terms, list):
            brand_terms = []

        by_i[i] = {
            "ss_product": ss_prod,
            "product_category": cat_det,
            "sentiment": str(it.get("sentiment", "Neutral")).strip(),
            "topic": str(it.get("topic", "Others")).strip(),
            "subtopic": str(it.get("subtopic", "Others")).strip(),
            "brand_terms": brand_terms,
        }

    out = []
    for i in range(len(texts)):
        out.append(by_i.get(i, {
            "ss_product": "No specific product",
            "product_category": "Others",
            "sentiment": "Neutral",
            "topic": "Others",
            "subtopic": "Others",
            "brand_terms": []
        }))

    if sleep:
        time.sleep(sleep)
    return out


# ========= 4) Helpers: open excel, find author column =========
def open_excel_file(path: str) -> pd.ExcelFile:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        return pd.ExcelFile(path, engine="openpyxl")
    raise ValueError(f"Unsupported file extension '{ext}'. Use a decrypted .xlsx")

def find_author_column(df: pd.DataFrame) -> str | None:
    # robust: supports AuthorName
    targets = {"author", "username", "authorname", "author name"}
    for c in df.columns:
        norm = re.sub(r"[\s_]+", "", str(c).strip().lower())
        if norm in targets:
            return c
    return None

def find_replies_column(df: pd.DataFrame) -> str | None:
    # flexible
    keys = ["repliescount", "replycount", "commentcount", "comments", "replies"]
    for c in df.columns:
        norm = re.sub(r"[\s_]+", "", str(c).strip().lower())
        if norm in keys:
            return c
    return None


# ========= 5) PIPELINE (with progress) =========
def run_pipeline(in_path: str,
                 text_col_pref=("Full text (EN)", "Combined Text (EN)"),
                 verbose: bool = True) -> str:
    xl = open_excel_file(in_path)
    processed = {}

    total_sheets = len(xl.sheet_names)
    if verbose:
        print(f"📘 Input file: {in_path}")
        print(f"📄 Sheets: {total_sheets} -> {', '.join(xl.sheet_names)}")
        print(f"⭐ Samsung Stars loaded (canon): {len(SAMSUNG_STARS)}")

    t0 = time.time()

    for idx, sh in enumerate(xl.sheet_names, start=1):
        t_sheet = time.time()
        df = xl.parse(sh)
        n_rows = len(df)

        if verbose:
            print(f"\n[{idx}/{total_sheets}] ▶ Sheet '{sh}' ({n_rows} rows)")

        # Text column selection
        text_col = next((c for c in text_col_pref if c in df.columns), None)
        if not text_col:
            # attempt build Combined Text (EN)
            title_col = next((c for c in df.columns if "title" in c.lower()), None)
            body_col  = next((c for c in df.columns if any(k in c.lower() for k in ["full text","snippet","content","body"])), None)
            df["Combined Text (EN)"] = df.apply(
                lambda r: " ".join([s for s in [str(r.get(title_col,"")), str(r.get(body_col,""))] if str(s).strip()]).strip(),
                axis=1
            )
            text_col = "Combined Text (EN)"

        texts = df[text_col].fillna("").astype(str).tolist()

        # AI classify
        if verbose:
            print(f"   - Classifying via {MODEL} ... ", end="", flush=True)
        t_cls = time.time()
        rows = classify_batch_json_mode_ai(texts)
        if verbose:
            print(f"done ({time.time()-t_cls:.1f}s)")

        df["SS Product"]       = [r["ss_product"] for r in rows]
        df["Product Category"] = [r["product_category"] for r in rows]
        df["Sentiment"]        = [r["sentiment"] for r in rows]
        df["Topic"]            = [r["topic"] for r in rows]
        df["Subtopic"]         = [r["subtopic"] for r in rows]
        df["Brand Terms"]      = ["; ".join(r["brand_terms"]) for r in rows]

        # Posted By (AuthorName supported)
        author_c = find_author_column(df)
        if verbose:
            print(f"   - Posted By from: {author_c if author_c else '(none -> Member)'}")
        df["Posted By"] = df[author_c].fillna("").astype(str).apply(classify_posted_by) if author_c else "Member"

        # Replied (Y/N)
        replies_c = find_replies_column(df)
        if replies_c:
            def replied_flag(v):
                try:
                    return "N" if float(v) == 0 else "Y"
                except Exception:
                    return "N"
            df["Replied (Y/N)"] = df[replies_c].apply(replied_flag)
            if verbose:
                zero = (pd.to_numeric(df[replies_c], errors="coerce").fillna(0) == 0).sum()
                print(f"   - Replied (Y/N) from '{replies_c}' (zero: {zero})")
        else:
            df["Replied (Y/N)"] = "N"
            if verbose:
                print("   - Replied (Y/N): replies column not found -> default N")

        processed[sh] = df
        if verbose:
            print(f"✅ Finished '{sh}' in {time.time()-t_sheet:.1f}s")

    out_path = os.path.splitext(in_path)[0] + "_classified_ai.xlsx"
    if verbose:
        print(f"\n💾 Writing output → {out_path}")

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        for sh, df in processed.items():
            df.to_excel(w, sheet_name=sh, index=False)

    if verbose:
        print(f"🎉 Done in {time.time()-t0:.1f}s")
    return out_path


# ========= 6) RUN =========
if __name__ == "__main__":
    # IMPORTANT: use your decrypted input excel here
    in_path = r"C:\Users\xueming.y\Desktop\test2.xlsx"
    print("🚀 Running Samsung Members classification...")
    out_path = run_pipeline(in_path, verbose=True)
    print("📦 Saved:", out_path)