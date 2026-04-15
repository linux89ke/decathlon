"""
Decathlon Product Lookup
Fixes & improvements in this version:
 - extract_uk_size now captures full range (UK 20-22, not just UK 20)
 - Other mode: variation comes from 'size' column (not 'variation'), dots only when empty
 - Price_KES always 100,000; Stock always 0
 - Product name colour append: checks if colour is anywhere in name (not just endswith)
 - Primary Category shown in full on front-end; Additional Category hidden
 - sizes.txt loaded from project folder automatically (no upload needed)
 - Front-end preview table shows final export look with per-row size override dropdowns (fashion)
 - Rows with size missing from sizes.txt are highlighted red in preview
 - Template export writes only the Upload Template sheet
 - IMAGE FIX: images are now packed sequentially (no blank gaps) — skips empty picture_1/2/etc.
 - PERF: keyword scoring vectorised with numpy; brand match cached per unique brand string;
         category batch pre-resolved once; is_size_missing uses a cached frozenset
 - BULLETPROOF HEADERS: Export mapping completely ignores spaces, underscores, and capitalization.
 - MASTER MAPPING: Description pulls directly from the 'description' column.
 - SIZE/VARIATION STRICT: Physically deletes the unused column from the template file so they never co-exist.
 - CATEGORY FORMAT: Category now exports as "CODE - FULL PATH".
 - AUTO-CREATE COLUMNS: If any required column is completely missing from the template, the script physically creates it.
 - DUPLICATE COLUMNS FIXED: Removed redundant price/stock assignments.
"""

import os, io, re, json, asyncio
from typing import Optional
import numpy as np
import streamlit as st
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill

try:
    from openai import AsyncOpenAI
    ZUMA_AVAILABLE = True
except ImportError:
    ZUMA_AVAILABLE = False

ZUMA_BASE_URL = "https://ai-gateway.zuma.jumia.com/v1"

st.set_page_config(page_title="Decathlon Product Lookup", page_icon="", layout="wide")

# Track session runs without clearing caches — cached data (TF-IDF index,
# reference data) is shared across sessions for fast first load.
# Use the "Clear Cache & Reset" button in the sidebar for a manual flush.
if "run_id" not in st.session_state:
    st.session_state["run_id"] = 1
st.markdown("""
<style>
h1 { color: #0082C3; }
.tag {
  display:inline-block; background:#0082C3; color:white;
  border-radius:4px; padding:2px 8px; font-size:12px; margin:2px;
}
.ai-badge {
  display:inline-block; background:linear-gradient(90deg,#f55036,#ff8c00);
  color:white; border-radius:12px; padding:2px 10px;
  font-size:11px; font-weight:700; margin-left:6px;
}
.kw-badge {
  display:inline-block; background:#0082C3; color:white;
  border-radius:12px; padding:2px 10px;
  font-size:11px; font-weight:700; margin-left:6px;
}
</style>
""", unsafe_allow_html=True)

st.title("Decathlon Product Lookup")
st.markdown("Search by SKU number — view details, images, and **download a filled upload template**.")

# ── Constants ─────────────────────────────────────────────────────────────────
IMAGE_COLS   = ["OG_image"] + [f"picture_{i}" for i in range(1, 11)]
TEMPLATE_PATH = "product-creation-template.xlsx"
DECA_CAT_PATH = "deca_cat.xlsx"
MASTER_PATH   = "Decathlon Working File Split.xlsx"
SIZES_PATH    = "sizes.txt"

MASTER_TO_TEMPLATE = {
    "product_name":    "Name",
    "description":     "Description",  
    "sku_num_sku_r3":  "SellerSKU",
    "brand_name":      "Brand",
    "bar_code":        "GTIN_Barcode",
    "color":           "color",
    "model_label":     "model",
    # Images are handled dynamically in build_template to skip blanks
}

# Template image slot names in order
TEMPLATE_IMAGE_SLOTS = ["MainImage"] + [f"Image{i}" for i in range(2, 9)]

CATEGORY_MATCH_FIELDS = [
    "family", "type", "department_label", "nature_label",
    "proposed_brand_name", "brand_name", "color", "channable_gender",
    "size", "keywords", "description", "business_weight", "product_name",
]

ZUMA_SYSTEM_CAT = """You are a product categorization expert for a sports retailer.
Given a product description and candidate category paths, pick the {top_n} best matches.
Consider brand, product type, gender, sport, and age group.

Respond with JSON only:
{{
 "categories": [
  {{"category":"<full path>","score": 0.95}},
  ...
 ]
}}

Rules:
- Return exactly {top_n} categories ordered by confidence descending
- Only pick from the provided candidate list - never invent categories
- Scores are floats 0.0-1.0
- JSON only, nothing else"""

ZUMA_SYSTEM_DESC = """You are a product copywriter for a sports retail marketplace.
Given product details, write exactly 3 short bullet points (each max 15 words) that highlight
the key features a buyer cares about. Focus on: sport/use-case, key benefit or material, target user.
Do NOT start with "Our team" or "Our designers". Be specific — mention the product name or sport.
Respond with JSON only:
{{"bullets": ["bullet 1","bullet 2","bullet 3"]}}
JSON only, nothing else."""


# =============================================================================
# UK SIZE EXTRACTION — fixed to capture full range e.g. "UK 20-22"
# =============================================================================

_UK_SIZE_PATTERNS = [
    # Full range first: "UK 20-22", "UK 6-8"
    re.compile(r'\b(UK\s*\d{1,2}(?:\.\d)?\s*[-–]\s*\d{1,2})\b', re.IGNORECASE),
    # Single number: "UK 10", "UK 29"
    re.compile(r'\b(UK\s*\d{1,2}(?:\.\d)?)\b', re.IGNORECASE),
]

_CHILDREN_AGE_PATTERN = re.compile(
    r'(\d{1,2})\s*-\s*(\d{1,2})\s*(?:years?|yrs?)',
    re.IGNORECASE,
)


def extract_uk_size(raw: str) -> Optional[str]:
    """Extract a clean UK size label from a messy size string.
    Returns full range if present (e.g. 'UK 20-22'), else single value."""
    if not raw:
        return None
    cleaned = re.sub(r'"+', '', raw).strip()
    for pat in _UK_SIZE_PATTERNS:
        m = pat.search(cleaned)
        if m:
            # Normalise spacing: ensure "UK " prefix
            val = re.sub(r'^(UK)\s*', 'UK ', m.group(1), flags=re.IGNORECASE)
            return val.strip()
    return None


def parse_valid_sizes(path: str) -> list:
    """Load sizes.txt — one size per line, skip blanks/comments."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        return lines
    except FileNotFoundError:
        return []


# =============================================================================
# HELPERS
# =============================================================================

def _clean(val) -> str:
    if pd.isna(val) or str(val).strip() in ("", "-", "nan"):
        return ""
    return str(val).strip()


def _format_gtin(val) -> str:
    """Convert scientific notation barcodes to full integer string."""
    raw = str(val).strip()
    if not raw or raw.lower() in ("nan", ""):
        return ""
    try:
        return str(int(float(raw)))
    except (ValueError, OverflowError):
        return raw


# =============================================================================
# DATA LOADING
# =============================================================================

@st.cache_data(show_spinner="Loading category & brand reference data…")
def load_reference_data(file_bytes: bytes):
    wb_bytes = io.BytesIO(file_bytes)
    df_cat = pd.read_excel(wb_bytes, sheet_name="category", dtype=str)
    df_cat.columns = [c.strip() for c in df_cat.columns]
    df_cat = df_cat[df_cat["export_category"].notna() & (df_cat["export_category"].str.strip() != "")]
    df_cat["export_category"]    = df_cat["export_category"].str.strip()
    df_cat["category_name_lower"] = df_cat["category_name"].str.lower().str.strip()
    df_cat["Category Path lower"] = df_cat["Category Path"].str.lower().fillna("")
    df_cat["_path_tokens"] = df_cat["Category Path lower"].apply(
        lambda p: set(re.findall(r"[a-z]+", p))
    )
    wb_bytes.seek(0)
    df_brands = pd.read_excel(wb_bytes, sheet_name="brands", dtype=str, header=0)
    df_brands.columns = ["brand_entry"]
    df_brands = df_brands[df_brands["brand_entry"].notna()].copy()
    df_brands["brand_entry"]    = df_brands["brand_entry"].str.strip()
    df_brands["brand_name_lower"] = (
        df_brands["brand_entry"].str.split("-", n=1).str[-1].str.lower().str.strip()
    )
    return df_cat, df_brands


# Alternate column names that should be treated as the SKU column
_SKU_ALIASES = {"seller sku", "sellersku", "seller_sku", "sku", "sku_num_sku_r3"}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename alternate SKU column names to the canonical 'sku_num_sku_r3'."""
    rename = {}
    for col in df.columns:
        if col.lower().replace(" ", "_") in _SKU_ALIASES and col != "sku_num_sku_r3":
            rename[col] = "sku_num_sku_r3"
            break  # only rename the first match to avoid duplicates
    if rename:
        df = df.rename(columns=rename)
    return df


MASTER_PICKLE_PATH = "master_data.pkl"


def _master_mtime() -> float:
    """Return modification time of the bundled master file, or 0 if missing."""
    for path in [MASTER_PATH, MASTER_PATH.replace(".xlsx", ".csv")]:
        try:
            return os.path.getmtime(path)
        except OSError:
            continue
    return 0.0


@st.cache_data(show_spinner="Loading master product data…")
def load_master(file_bytes: bytes, is_csv: bool) -> pd.DataFrame:
    if is_csv:
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, encoding="latin-1")
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    return _normalise_columns(df)


def load_master_fast(is_uploaded: bool = False) -> pd.DataFrame:
    """
    Load the bundled master file using a pickle cache for speed.
    Falls back to reading the xlsx/csv directly if pickle is missing or stale.
    Only used for the bundled file — uploaded overrides always bypass this.
    """
    import pickle

    mtime = _master_mtime()

    # Try pickle first
    if os.path.exists(MASTER_PICKLE_PATH):
        try:
            with open(MASTER_PICKLE_PATH, "rb") as f:
                cached = pickle.load(f)
            if cached.get("mtime") == mtime:
                return cached["df"]
        except Exception:
            pass  # corrupt pickle — rebuild below

    # Read from xlsx / csv
    df = None
    for path, csv in [(MASTER_PATH, False), (MASTER_PATH.replace(".xlsx", ".csv"), True)]:
        try:
            raw = open(path, "rb").read()
            df  = load_master(raw, csv)
            break
        except FileNotFoundError:
            continue

    if df is None:
        return None

    # Save pickle
    try:
        with open(MASTER_PICKLE_PATH, "wb") as f:
            pickle.dump({"mtime": mtime, "df": df}, f)
    except Exception:
        pass

    return df


# =============================================================================
# TF-IDF INDEX
# =============================================================================

def _path_to_doc(path: str) -> str:
    parts = path.split("/")
    return " ".join(parts) + " " + " ".join(parts[-3:]) * 2


TFIDF_PICKLE_PATH = "tfidf_index.pkl"


def _cat_mtime() -> float:
    """Return deca_cat.xlsx modification time, or 0 if missing."""
    try:
        return os.path.getmtime(DECA_CAT_PATH)
    except OSError:
        return 0.0


@st.cache_resource(show_spinner="Loading category index…")
def build_tfidf_index(ref_bytes: bytes):
    import pickle
    from sklearn.feature_extraction.text import TfidfVectorizer

    mtime = _cat_mtime()

    # ── Try loading from pickle ────────────────────────────────────────────
    if os.path.exists(TFIDF_PICKLE_PATH):
        try:
            with open(TFIDF_PICKLE_PATH, "rb") as f:
                cached = pickle.load(f)
            if cached.get("mtime") == mtime:
                return (
                    cached["leaves"],
                    cached["vectorizer"],
                    cached["matrix"],
                    cached["path_to_export"],
                )
        except Exception:
            pass  # corrupt pickle — rebuild below

    # ── Build from scratch ─────────────────────────────────────────────
    df_cat, _ = load_reference_data(ref_bytes)
    all_paths = df_cat["Category Path"].dropna().astype(str).tolist()
    path_set  = set(all_paths)
    leaves    = [p for p in all_paths
                 if not any(other.startswith(p + "/") for other in path_set)]
    docs      = [_path_to_doc(p) for p in leaves]
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1, sublinear_tf=True)
    matrix    = vectorizer.fit_transform(docs)
    path_to_export = dict(zip(df_cat["Category Path"], df_cat["export_category"]))

    # ── Save pickle ───────────────────────────────────────────────────
    try:
        with open(TFIDF_PICKLE_PATH, "wb") as f:
            pickle.dump({
                "mtime":          mtime,
                "leaves":         leaves,
                "vectorizer":     vectorizer,
                "matrix":         matrix,
                "path_to_export": path_to_export,
            }, f)
    except Exception:
        pass  # pickle save failure is non-fatal

    return leaves, vectorizer, matrix, path_to_export


def tfidf_shortlist(queries: list, leaves, vectorizer, matrix, k: int = 30) -> list:
    from sklearn.metrics.pairwise import cosine_similarity
    qmat = vectorizer.transform(queries)
    sims = cosine_similarity(qmat, matrix)
    out = []
    for row in sims:
        top_idx = np.argsort(row)[::-1][:k]
        out.append([leaves[i] for i in top_idx if row[i] > 0])
    return out


# =============================================================================
# KEYWORD MATCHING
# =============================================================================

def _build_query_string(row: pd.Series) -> str:
    parts = []
    for f in CATEGORY_MATCH_FIELDS:
        val = row.get(f, "")
        if pd.notna(val) and str(val).strip() not in ("", "-", "nan"):
            parts.append(str(val).strip().lower())
    return " ".join(parts)


def keyword_match_batch(rows_df: pd.DataFrame, df_cat: pd.DataFrame) -> list:
    queries = [_build_query_string(row) for _, row in rows_df.iterrows()]
    cat_token_sets = df_cat["_path_tokens"].tolist()
    cat_depths     = np.array(df_cat["Category Path lower"].str.count("/").tolist(), dtype=np.float32)
    cat_names      = df_cat["category_name_lower"].tolist()
    cat_exports    = df_cat["export_category"].tolist()
    n_cats         = len(cat_exports)
    depth_bonus    = cat_depths * 0.1  # precompute once
    results = []
    for query in queries:
        if not query:
            results.append(("", ""))
            continue
        q_tokens = set(re.findall(r"[a-z]+", query))
        # Vectorised token-overlap + name bonus + depth
        token_scores = np.array([len(q_tokens & s) for s in cat_token_sets], dtype=np.float32)
        name_bonus   = np.array([2.0 if n in query else 0.0 for n in cat_names], dtype=np.float32)
        scores       = token_scores + name_bonus + depth_bonus
        top2         = np.argpartition(scores, -min(2, n_cats))[-min(2, n_cats):]
        top2         = top2[np.argsort(scores[top2])[::-1]]
        primary   = cat_exports[top2[0]] if scores[top2[0]] > 0 else ""
        secondary = cat_exports[top2[1]] if len(top2) > 1 and scores[top2[1]] > 0 else ""
        results.append((primary, secondary))
    return results


def keyword_match_category(row: pd.Series, df_cat: pd.DataFrame) -> tuple:
    return keyword_match_batch(pd.DataFrame([row]), df_cat)[0]


# =============================================================================
# VARIATION
# =============================================================================

def get_variation(
    row: pd.Series,
    is_fashion: bool = True,
    valid_sizes: Optional[list] = None,
    size_override: Optional[str] = None,
) -> str:
    """
    Fashion  → 'size' column, UK extraction, sizes.txt validation
    Other    → 'size' column directly; '...' only if empty/missing
    """
    raw_size = re.sub(r'"+', '', str(row.get("size", ""))).strip().rstrip(".")

    if not is_fashion:
        # Other: use size column as-is, fallback to variation col, dots if both empty
        if raw_size.lower() not in ("", "nan", "no size", "none"):
            return raw_size
        raw_var = re.sub(r'"+', '', str(row.get("variation", ""))).strip().rstrip(".")
        if raw_var.lower() not in ("", "nan", "no size", "none"):
            return raw_var
        return "..."

    # Fashion path
    if raw_size.lower() in ("", "nan", "no size", "none"):
        return size_override or "..."

    if size_override:
        return size_override

    # Direct match against sizes.txt
    if valid_sizes:
        raw_upper = raw_size.upper()
        for s in valid_sizes:
            if s.upper() == raw_upper:
                return s

    # UK range/size extraction
    uk = extract_uk_size(raw_size)
    if uk and valid_sizes:
        uk_upper = uk.upper()
        for s in valid_sizes:
            if s.upper() == uk_upper:
                return s
        return uk  # extracted but not in list

    # Partial match
    if valid_sizes:
        raw_lower = raw_size.lower()
        for s in valid_sizes:
            if s.lower() in raw_lower or raw_lower in s.lower():
                return s

    return raw_size  # best-effort fallback


@st.cache_data
def _valid_sizes_upper_set(sizes_tuple: tuple) -> frozenset:
    """Build a frozen upper-case set from valid_sizes once and cache it."""
    return frozenset(s.upper() for s in sizes_tuple)


def is_size_missing(computed_variation: str, valid_sizes: list) -> bool:
    """Return True if the computed variation is not in sizes.txt (flags red row)."""
    if not valid_sizes:
        return False
    if computed_variation in ("...", ""):
        return True
    return computed_variation.upper() not in _valid_sizes_upper_set(tuple(valid_sizes))


# =============================================================================
# SHORT DESCRIPTION
# =============================================================================

GENDER_MAP = {
    "MEN'S": "Men", "WOMEN'S": "Women", "BOYS'": "Boys", "GIRLS'": "Girls",
    "MEN": "Men", "WOMEN": "Women", "UNISEX": "Unisex", "NO GENDER": "",
    "HORSE": "",
}

_QUALITY_KEYWORDS = [
    "comfortable", "comfort", "lightweight", "light weight", "durable", "durability",
    "breathable", "breathability", "flexible", "flexibility", "waterproof", "water-resistant",
    "quick-dry", "quick dry", "moisture-wicking", "wicking", "anti-odour", "anti-odor",
    "odour-resistant", "stretch", "stretchable", "supportive", "support", "cushioned",
    "cushioning", "padded", "ergonomic", "adjustable", "reflective", "insulated",
    "warm", "cool", "softness", "soft", "reinforced", "abrasion-resistant", "non-slip",
    "grip", "ventilated", "ventilation", "seamless", "compression", "packable",
    "ultra-light", "high-performance", "performance", "protection", "protective",
]


def _extract_quality_phrases(desc: str, max_phrases: int = 2) -> list:
    if not desc:
        return []
    found = []
    used_words: set = set()
    desc_lower = desc.lower()
    for kw in _QUALITY_KEYWORDS:
        if kw in desc_lower:
            idx = desc_lower.index(kw)
            snippet = desc[idx:]
            sentence_end = re.search(r'[.!?]', snippet)
            if sentence_end:
                snippet = snippet[:sentence_end.start()]
            words_after = snippet.split()[:6]
            phrase = " ".join(words_after).rstrip(".,;:- ")
            phrase_words = set(phrase.lower().split())
            if phrase and len(phrase_words) >= 3 and len(phrase_words & used_words) < 2:
                found.append(phrase.capitalize())
                used_words |= phrase_words
            if len(found) >= max_phrases:
                break
    return found


def rule_based_short_desc(row: pd.Series) -> str:
    bullets = []
    brand  = _clean(row.get("brand_name", "")).title()
    dept   = _clean(row.get("department_label", "")).replace("/", "·").title()
    ptype  = _clean(row.get("type", "")).title()
    sport  = dept if dept else ptype
    g_raw  = _clean(row.get("channable_gender", "")).split("|")[0].strip().upper()
    gender = GENDER_MAP.get(g_raw, g_raw.title())
    b1_parts = [p for p in [brand, sport, gender] if p]
    if b1_parts:
        bullets.append(" · ".join(b1_parts))
    desc = _clean(row.get("description", ""))  
    quality_phrases = _extract_quality_phrases(desc, max_phrases=2)
    if quality_phrases:
        bullets.append(" · ".join(quality_phrases))
    elif desc:
        sentences = [s.strip() for s in re.split(r"[.!?]", desc) if len(s.strip()) > 20]
        feature = next(
            (s for s in sentences if not re.match(r"our (team|design)", s, re.I)),
            sentences[0] if sentences else "",
        )
        if feature:
            trunc = feature[:110].rsplit(" ", 1)[0] if len(feature) > 110 else feature
            bullets.append(trunc.capitalize())
    color  = _clean(row.get("color", "")).split("|")[0].strip().title()
    size   = re.sub(r'"+', "", _clean(row.get("size", ""))).strip().rstrip(".")
    nature = _clean(row.get("nature_label", "")).title()
    if color and size and size.lower() != "no size":
        bullets.append(f"Colour: {color} · Size: {size}")
    elif color and nature:
        bullets.append(f"{nature} · {color}")
    elif color:
        bullets.append(f"Colour: {color}")
    elif size and size.lower() != "no size":
        bullets.append(f"Size: {size}")
    elif nature:
        bullets.append(nature)
    if not bullets:
        return ""
    items = "".join(f"<li>{b}</li>" for b in bullets[:3])
    return f"<ul>{items}</ul>"


# =============================================================================
# AI MATCHING
# =============================================================================

async def _async_rerank(idx, query, candidates, client, model, top_n, sem, task_type="cat"):
    async with sem:
        try:
            if task_type == "cat":
                cand_list = "\n".join(f"- {c}" for c in candidates)
                sys_msg   = ZUMA_SYSTEM_CAT.format(top_n=top_n)
                user_msg  = f"Product: {query}\n\nCandidates:\n{cand_list}"
            else:
                sys_msg   = ZUMA_SYSTEM_DESC
                user_msg  = f"Product details: {query}"
            resp = await client.chat.completions.create(
                model=model,
                temperature=0.15,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": sys_msg},
                    {"role": "user",   "content": user_msg},
                ],
            )
            raw  = resp.choices[0].message.content.strip()
            data = json.loads(raw)
            return idx, data
        except Exception as e:
            return idx, {"error": str(e)}


async def _parallel_tasks(items, client, model, sem, task_type):
    tasks = [
        _async_rerank(i, q, c, client, model, 2, sem, task_type)
        for i, (q, c) in enumerate(items)
    ]
    raw = await asyncio.gather(*tasks)
    return [r for _, r in sorted(raw, key=lambda x: x[0])]


def ai_batch(items, api_key, model, concurrency, task_type="cat"):
    async def _run():
        client = AsyncOpenAI(api_key=api_key, base_url=ZUMA_BASE_URL)
        sem    = asyncio.Semaphore(concurrency)
        return await _parallel_tasks(items, client, model, sem, task_type)
    return asyncio.run(_run())


def ai_match_categories(rows_df, leaves, vectorizer, matrix, path_to_export,
                         api_key, model, shortlist_k=30, concurrency=10):
    def _resolve(cat_path: str) -> str:
        if cat_path in path_to_export:
            return path_to_export[cat_path]
        for p, ex in path_to_export.items():
            if p.endswith(cat_path) or cat_path.endswith(p):
                return ex
        return cat_path

    model_to_query: dict = {}
    model_order: list    = []
    for _, row in rows_df.iterrows():
        mc = str(row.get("model_code", "")).strip()
        if mc and mc not in model_to_query:
            group = rows_df[rows_df["model_code"] == mc]
            model_to_query[mc] = _build_query_string(group.iloc[0])
            model_order.append(mc)

    unique_queries  = [model_to_query[mc] for mc in model_order]
    candidates_list = tfidf_shortlist(unique_queries, leaves, vectorizer, matrix, shortlist_k)
    items           = list(zip(unique_queries, candidates_list))
    raw_preds       = ai_batch(items, api_key, model, concurrency, task_type="cat")

    model_to_cats: dict = {}
    for mc, data in zip(model_order, raw_preds):
        cats      = data.get("categories", [])
        primary   = _resolve(cats[0]["category"]) if len(cats) > 0 else ""
        secondary = _resolve(cats[1]["category"]) if len(cats) > 1 else ""
        model_to_cats[mc] = (primary, secondary)

    results = []
    for _, row in rows_df.iterrows():
        mc = str(row.get("model_code", "")).strip()
        if mc and mc in model_to_cats:
            results.append(model_to_cats[mc])
        else:
            q  = _build_query_string(row)
            c  = tfidf_shortlist([q], leaves, vectorizer, matrix, shortlist_k)[0]
            rd = ai_batch([(q, c)], api_key, model, 1, task_type="cat")[0]
            cats      = rd.get("categories", [])
            primary   = _resolve(cats[0]["category"]) if len(cats) > 0 else ""
            secondary = _resolve(cats[1]["category"]) if len(cats) > 1 else ""
            results.append((primary, secondary))

    return results, model_to_cats


def _build_desc_query_per_model(group_df: pd.DataFrame) -> str:
    row   = group_df.iloc[0]
    parts = [
        _clean(row.get("product_name", "")),
        _clean(row.get("department_label", "")),
        _clean(row.get("brand_name", "")),
        _clean(row.get("channable_gender", "")).split("|")[0].strip(),
        _clean(row.get("description", ""))[:300],  
        _clean(row.get("keywords", ""))[:100],
    ]
    return "|".join(p for p in parts if p)


def ai_short_descriptions(rows_df, api_key, model, concurrency=10):
    model_queries: dict = {}
    model_repr:    dict = {}
    for i, (_, row) in enumerate(rows_df.iterrows()):
        mc = str(row.get("model_code", "")).strip()
        if mc and mc not in model_queries:
            group = rows_df[rows_df["model_code"] == mc]
            model_queries[mc] = _build_desc_query_per_model(group)
            model_repr[mc]    = i

    unique_models = list(model_queries.keys())
    items         = [(model_queries[mc], []) for mc in unique_models]
    raw_results   = ai_batch(items, api_key, model, concurrency, task_type="desc")

    model_to_desc: dict = {}
    for mc, data in zip(unique_models, raw_results):
        if "error" in data:
            fallback_row = rows_df.iloc[model_repr[mc]]
            model_to_desc[mc] = rule_based_short_desc(fallback_row)
        else:
            bullets = data.get("bullets", [])
            items_  = "".join(f"<li>{b}</li>" for b in bullets[:3])
            model_to_desc[mc] = f"<ul>{items_}</ul>"

    descs = []
    for _, row in rows_df.iterrows():
        mc = str(row.get("model_code", "")).strip()
        if mc and mc in model_to_desc:
            descs.append(model_to_desc[mc])
        else:
            descs.append(rule_based_short_desc(row))
    return descs


# =============================================================================
# BRAND MATCHING
# =============================================================================

def match_brand(raw: str, df_brands: pd.DataFrame) -> str:
    if not raw or pd.isna(raw):
        return ""
    needle = str(raw).strip().lower()
    # Build lookup dicts once per unique df_brands object using a cache keyed by id
    exact = df_brands[df_brands["brand_name_lower"] == needle]
    if not exact.empty:
        return exact.iloc[0]["brand_entry"]
    partial = df_brands[df_brands["brand_name_lower"].str.contains(needle, regex=False, na=False)]
    if not partial.empty:
        return partial.iloc[0]["brand_entry"]
    # Check if any brand name is contained within the needle
    match = df_brands[df_brands["brand_name_lower"].apply(lambda b: b in needle)]
    if not match.empty:
        return match.iloc[0]["brand_entry"]
    return str(raw).strip()


# =============================================================================
# TEMPLATE BUILDER
# =============================================================================

RED_FILL = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")


def build_template(
    results_df,
    df_cat,
    df_brands,
    ai_categories,
    short_descs,
    is_fashion: bool = True,
    valid_sizes: Optional[list] = None,
    size_overrides: Optional[dict] = None,   # dict keyed by df index -> size string
) -> bytes:
    wb = load_workbook(TEMPLATE_PATH)

    # Keep only the Upload Template sheet
    sheets_to_remove = [s for s in wb.sheetnames if s != "Upload Template"]
    for s in sheets_to_remove:
        del wb[s]

    ws = wb["Upload Template"]

    # ── BULLETPROOF HEADER MAPPING ──
    header_map = {}
    for col_idx in range(1, ws.max_column + 1):
        val = ws.cell(row=1, column=col_idx).value
        if val:
            norm_val = re.sub(r'[^a-z0-9]', '', str(val).lower())
            header_map[norm_val] = col_idx

    # ── STRICT SEPARATION: DELETE THE UNUSED COLUMN ──
    # If the template file comes with BOTH columns pre-built, we physically delete the one we don't need.
    unused_col_key = "variation" if is_fashion else "size"
    if unused_col_key in header_map:
        ws.delete_cols(header_map[unused_col_key])
        
        # Re-build header map because deleting a column shifts all columns to the left!
        header_map = {}
        for col_idx in range(1, ws.max_column + 1):
            val = ws.cell(row=1, column=col_idx).value
            if val:
                norm_val = re.sub(r'[^a-z0-9]', '', str(val).lower())
                header_map[norm_val] = col_idx

    # ── FORCE MISSING COLUMNS GUARANTEE ──
    # If the template lacks the active column entirely, we explicitly create it at the end.
    active_col_key = "size" if is_fashion else "variation"
    active_col_label = "Size" if is_fashion else "Variation"
    
    if active_col_key not in header_map:
        new_col = ws.max_column + 1
        ws.cell(row=1, column=new_col).value = active_col_label
        header_map[active_col_key] = new_col
        
    current_max_col = ws.max_column

    hfont      = ws.cell(row=1, column=1).font
    data_font  = Font(name=hfont.name or "Calibri", size=hfont.size or 11)
    data_align = Alignment(vertical="center")

    # ParentSKU: first SKU per model_code
    model_to_first_sku: dict = {}
    for _, r in results_df.iterrows():
        mc  = str(r.get("model_code", "")).strip()
        sku = str(r.get("sku_num_sku_r3", "")).strip()
        if mc and sku and mc not in model_to_first_sku:
            model_to_first_sku[mc] = sku

    # export_code -> full Category Path
    if df_cat is not None:
        exp_to_fullpath: dict = {}
        for _, _cr in df_cat.iterrows():
            _exp = str(_cr.get("export_category", "")).strip()
            _fp  = str(_cr.get("Category Path", "")).strip()
            if _exp and _fp and _exp not in exp_to_fullpath:
                exp_to_fullpath[_exp] = _fp
    else:
        exp_to_fullpath = {}

    # Pre-resolve keyword categories in one batch when no AI categories provided
    if not ai_categories and df_cat is not None:
        _kw_cats = keyword_match_batch(results_df, df_cat)
    else:
        _kw_cats = None

    # Brand match cache to avoid repeated lookups for the same brand string
    _brand_cache: dict = {}

    for i, (idx, src_row) in enumerate(results_df.iterrows()):
        row_idx  = i + 2
        row_data = {}

        # Standard fields (non-image)
        for master_col, tmpl_col in MASTER_TO_TEMPLATE.items():
            val = src_row.get(master_col, "")
            if pd.notna(val) and str(val).strip() not in ("", "nan"):
                row_data[tmpl_col] = str(val).strip()

        # Images: collect all non-empty URLs from IMAGE_COLS in order
        img_urls = [
            str(src_row[c]).strip()
            for c in IMAGE_COLS
            if c in src_row and pd.notna(src_row[c]) and str(src_row[c]).strip() not in ("", "nan")
        ]
        for slot, url in zip(TEMPLATE_IMAGE_SLOTS, img_urls):
            row_data[slot] = url

        # ParentSKU
        mc = str(src_row.get("model_code", "")).strip()
        if mc and mc in model_to_first_sku:
            row_data["ParentSKU"] = model_to_first_sku[mc]

        # GTIN
        gtin = _format_gtin(src_row.get("bar_code", ""))
        if gtin:
            row_data["GTIN_Barcode"] = gtin

        # Product name: append colour only if colour not already anywhere in name
        product_name = str(src_row.get("product_name", "")).strip()
        color_raw    = str(src_row.get("color", "")).strip()
        color        = color_raw.split("|")[0].strip()

        if product_name and color:
            if color.lower() not in product_name.lower():
                row_data["Name"] = f"{product_name} - {color.title()}"
            else:
                row_data["Name"] = product_name
        elif product_name:
            row_data["Name"] = product_name

        # product_weight
        bw = str(src_row.get("business_weight", "")).strip()
        if bw and bw.lower() not in ("", "nan"):
            row_data["product_weight"] = re.sub(r'\s*kg\s*$', '', bw, flags=re.IGNORECASE).strip()

        # package_content: "Name - Size"
        size_val = re.sub(r'"+', '', str(src_row.get("size", ""))).strip().rstrip(".")
        if size_val.lower() not in ("", "nan", "no size"):
            pkg_name = row_data.get("Name", product_name)
            row_data["package_content"] = f"{pkg_name} - {size_val}"

        # Brand
        raw_brand = src_row.get("brand_name", "")
        if pd.notna(raw_brand) and str(raw_brand).strip():
            brand_key = str(raw_brand).strip()
            if brand_key not in _brand_cache:
                _brand_cache[brand_key] = match_brand(brand_key, df_brands)
            row_data["Brand"] = _brand_cache[brand_key]

        # Category — formatted strictly as CODE - FULL PATH
        if ai_categories and i < len(ai_categories):
            primary_code, secondary_code = ai_categories[i]
        elif _kw_cats:
            primary_code, secondary_code = _kw_cats[i]
        else:
            primary_code, secondary_code = ("", "")

        primary_full   = exp_to_fullpath.get(primary_code, primary_code)
        secondary_full = exp_to_fullpath.get(secondary_code, secondary_code)

        if primary_code and primary_full:
            row_data["PrimaryCategory"] = f"{primary_code} - {primary_full}" if primary_code != primary_full else primary_full
        elif primary_full:
            row_data["PrimaryCategory"] = primary_full
            
        if secondary_code and secondary_full:
            row_data["AdditionalCategory"] = f"{secondary_code} - {secondary_full}" if secondary_code != secondary_full else secondary_full
        elif secondary_full:
            row_data["AdditionalCategory"] = secondary_full

        # Size/Variation
        per_row_override = (size_overrides or {}).get(idx)
        computed_var = get_variation(
            src_row,
            is_fashion=is_fashion,
            valid_sizes=valid_sizes,
            size_override=per_row_override,
        )

        if is_fashion:
            row_data["Size"] = computed_var   
        else:
            row_data["Variation"] = computed_var   

        # Price_KES / Stock
        row_data["Price_KES"] = "100000"
        row_data["Stock"]     = "0"

        # Short description
        if short_descs and i < len(short_descs) and short_descs[i]:
            row_data["short_description"] = short_descs[i]

        flag_red = is_fashion and is_size_missing(computed_var, valid_sizes or [])

        # ── AUTO-CREATE AND WRITE CELLS ──
        for tmpl_col, value in row_data.items():
            norm_tmpl_col = re.sub(r'[^a-z0-9]', '', str(tmpl_col).lower())
            
            # If the column doesn't exist in the template, create it dynamically
            if norm_tmpl_col not in header_map:
                current_max_col += 1
                header_cell = ws.cell(row=1, column=current_max_col)
                header_cell.value = tmpl_col
                header_cell.font = Font(bold=True) # Give the new header nice styling
                header_map[norm_tmpl_col] = current_max_col
            
            cell           = ws.cell(row=row_idx, column=header_map[norm_tmpl_col])
            cell.value     = value
            cell.font      = data_font
            cell.alignment = data_align
            
            # Highlight missing fashion sizes in red
            if flag_red and tmpl_col in ("Size", "Variation"):
                cell.fill = RED_FILL

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# =============================================================================
# SIDEBAR
# =============================================================================

with st.sidebar:
    st.header("Master Data")
    _bundled_exists = any(
        os.path.exists(p) for p in [MASTER_PATH, MASTER_PATH.replace(".xlsx", ".csv")]
    )
    if _bundled_exists:
        st.success("Master file loaded from system")
        with st.expander("Override with a different file"):
            uploaded_master = st.file_uploader("Working file (.xlsx or .csv)", type=["xlsx", "csv"])
    else:
        uploaded_master = st.file_uploader("Working file (.xlsx or .csv)", type=["xlsx", "csv"])

    st.markdown("---")
    # Keys that belong to working data (search results, overrides, AI output).
    # Reference data caches (category/brand files, TF-IDF index) are intentionally
    # left intact so they don't need to be reloaded on every reset.
    _WORKING_STATE_KEYS = {
        "run_id", "size_overrides", "cat_overrides",
        "ai_categories", "short_descs", "combined",
    }

    if st.button("Clear Working Data", use_container_width=True,
                 help="Resets search results and overrides. Category & brand files are kept loaded."):
        # Only clear working session state — leave reference caches alone
        for key in list(st.session_state.keys()):
            if key in _WORKING_STATE_KEYS or key.startswith(("size_fix_", "prim_", "cat_search")):
                del st.session_state[key]
        st.rerun()

    st.markdown("---")
    st.header("Category Matching")
    use_ai_matching = st.toggle(
        "AI matching (Zuma)",
        value=False,
        help="OFF = fast keyword/TF-IDF. ON = TF-IDF shortlist + Zuma AI rerank.",
    )

    if use_ai_matching:
        if not ZUMA_AVAILABLE:
            st.error("Install openai: `pip install openai`")
            use_ai_matching = False
        else:
            st.markdown('<span class="ai-badge">AI MODE ON</span>', unsafe_allow_html=True)
            show_key     = st.checkbox("Show key while typing", value=False)
            zuma_api_key = st.text_input(
                "Zuma AI API key",
                type="default" if show_key else "password",
                value=os.environ.get("ZUMA_API_KEY", ""),
                placeholder="Paste your jvk_... key here",
            )
            if zuma_api_key and not zuma_api_key.startswith("jvk_"):
                st.warning("Zuma keys usually start with `jvk_` — double-check.")
            st.caption(f"Gateway: `{ZUMA_BASE_URL}`")
            zuma_model  = st.selectbox(
                "Model",
                ["claude-sonnet-4.5", "claude-opus-4.5", "claude-haiku-4.5"],
                index=0,
            )
            shortlist_k  = st.slider("Shortlist size", 10, 50, 30)
            concurrency  = st.slider("Parallel Zuma requests", 1, 30, 10)
            st.markdown("---")
            ai_short_desc = st.toggle("AI short descriptions (Zuma)", value=True)
    else:
        st.markdown('<span class="kw-badge">KEYWORD MODE</span>', unsafe_allow_html=True)
        st.caption("Instant vectorised TF-IDF keyword matching. No API key needed.")
        zuma_api_key  = ""
        zuma_model    = "claude-sonnet-4.5"
        shortlist_k   = 30
        concurrency   = 10
        ai_short_desc = False

    st.markdown("---")
    st.header("Product Type")
    product_type = st.radio(
        "Product type",
        ["Fashion", "Other"],
        index=0,
        horizontal=True,
        help=(
            "Fashion: uses the 'size' column with UK-size extraction + sizes.txt validation.\n"
            "Other: uses the 'size' column directly; '...' only when size is empty."
        ),
    )
    is_fashion = product_type == "Fashion"

    # Load valid sizes from project folder automatically
    valid_sizes: list = parse_valid_sizes(SIZES_PATH)
    if valid_sizes:
        st.sidebar.info(f"sizes.txt loaded: {len(valid_sizes)} sizes")
    else:
        st.sidebar.warning("sizes.txt not found in project folder.")

    st.markdown("---")
    st.header("Search Fields")
    also_search_name = st.checkbox("Also search by product name", value=False)


# =============================================================================
# LOAD REFERENCE DATA
# =============================================================================

try:
    ref_bytes = open(DECA_CAT_PATH, "rb").read()
    st.sidebar.success("deca_cat.xlsx loaded")
except FileNotFoundError:
    ref_bytes = None
    st.sidebar.error(f"`{DECA_CAT_PATH}` not found. Place it alongside app.py and restart.")

if ref_bytes:
    df_cat, df_brands = load_reference_data(ref_bytes)
    st.sidebar.success(f"{len(df_cat):,} categories · {len(df_brands)} brands")
    leaves, vectorizer, tfidf_matrix, path_to_export = build_tfidf_index(ref_bytes)
else:
    df_cat = df_brands = leaves = vectorizer = tfidf_matrix = path_to_export = None


# =============================================================================
# LOAD MASTER DATA
# =============================================================================

if uploaded_master:
    # User has explicitly uploaded a file — read it directly (no pickle)
    master_bytes = uploaded_master.read()
    is_csv       = uploaded_master.name.endswith(".csv")
    df_master    = load_master(master_bytes, is_csv)
    st.sidebar.success(f"{len(df_master):,} product rows loaded")
else:
    # Use pickle-backed fast loader for the bundled master file
    df_master = load_master_fast()
    if df_master is None:
        st.error(f"Master file not found. Place '{MASTER_PATH}' in the same folder as app.py.")
        st.stop()
    st.sidebar.info(f"Bundled master · {len(df_master):,} rows")

img_cols_present = [c for c in IMAGE_COLS if c in df_master.columns]
data_cols        = [c for c in df_master.columns if c not in img_cols_present]


# =============================================================================
# SEARCH
# =============================================================================

def search(q: str) -> pd.DataFrame:
    mask = pd.Series(False, index=df_master.index)
    mask |= df_master["sku_num_sku_r3"].fillna("").str.strip() == q.strip()
    if also_search_name and "product_name" in df_master.columns:
        mask |= df_master["product_name"].fillna("").str.lower().str.contains(q.lower(), regex=False)
    return df_master[mask].copy()


# =============================================================================
# INPUT TABS
# =============================================================================

tab1, tab2, tab3 = st.tabs(["Upload a List", "Manual Entry", "Explore Categories"])
queries = []

with tab1:
    uploaded_list = st.file_uploader(
        "Upload file with SKU numbers",
        type=["xlsx", "csv", "txt"],
        help="One SKU per row. For Excel/CSV, SKUs must be in column A.",
    )
    if uploaded_list:
        ext = uploaded_list.name.rsplit(".", 1)[-1].lower()
        if ext == "txt":
            queries = [l.strip() for l in uploaded_list.read().decode().splitlines() if l.strip()]
        elif ext == "csv":
            q_df    = pd.read_csv(uploaded_list, header=None, dtype=str)
            queries = q_df.iloc[:, 0].dropna().str.strip().tolist()
        else:
            q_df    = pd.read_excel(uploaded_list, header=None, dtype=str)
            queries = q_df.iloc[:, 0].dropna().str.strip().tolist()
        st.success(f"Loaded **{len(queries)}** search terms")

with tab2:
    manual = st.text_area(
        "Enter one SKU number per line",
        height=160,
        placeholder="4273417\n4273418\n4273423",
    )
    manual_submitted = st.button("Search SKUs", type="primary", use_container_width=True)
    if manual_submitted and manual.strip():
        st.session_state["manual_queries"] = [q.strip() for q in manual.strip().splitlines() if q.strip()]
    if "manual_queries" in st.session_state and not manual_submitted:
        queries = st.session_state["manual_queries"]
    elif manual_submitted and manual.strip():
        queries = st.session_state["manual_queries"]

with tab3:
    st.subheader("Category Explorer")
    if df_cat is None:
        st.warning("deca_cat.xlsx not loaded — categories unavailable.")
    else:
        # Build a clean deduplicated display dataframe
        cat_display = df_cat[["Category Path", "export_category", "category_name"]].copy()
        cat_display.columns = ["Full Path", "Export Code", "Category Name"]
        cat_display = cat_display.drop_duplicates(subset=["Export Code"]).reset_index(drop=True)

        # Split path into level columns
        path_parts = cat_display["Full Path"].str.split("/", expand=True)
        n_levels = path_parts.shape[1]
        path_parts.columns = [f"L{i+1}" for i in range(n_levels)]
        cat_display = pd.concat([cat_display, path_parts], axis=1)

        # ── Stats bar ────────────────────────────────────────────────────────
        total_cats  = len(cat_display)
        n_l1        = cat_display["L1"].nunique()
        deepest     = cat_display["Full Path"].str.count("/").max() + 1
        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("Total categories", total_cats)
        sc2.metric("Top-level groups", n_l1)
        sc3.metric("Max depth", deepest)

        st.markdown("---")

        # ── View toggle ───────────────────────────────────────────────────────
        view_mode = st.radio(
            "View as",
            ["Tree (drill-down)", "Flat table"],
            horizontal=True,
            key="cat_view_mode",
        )

        # ── Keyword search ────────────────────────────────────────────────────
        cat_explore_search = st.text_input(
            "Search categories",
            placeholder="e.g. running, football, kids, hiking",
            key="cat_explore_search",
        )
        q_lower = cat_explore_search.strip().lower()

        # Apply keyword filter
        if q_lower:
            mask = (
                cat_display["Full Path"].str.lower().str.contains(q_lower, na=False) |
                cat_display["Category Name"].str.lower().str.contains(q_lower, na=False) |
                cat_display["Export Code"].str.lower().str.contains(q_lower, na=False)
            )
            filtered = cat_display[mask].reset_index(drop=True)
        else:
            filtered = cat_display.copy()

        st.caption(
            f"Showing **{len(filtered)}** / {total_cats} categor{'y' if len(filtered)==1 else 'ies'}"
            + (f" matching '{cat_explore_search}'" if q_lower else "")
        )

        # ── TREE VIEW ─────────────────────────────────────────────────────────
        if view_mode == "Tree (drill-down)":
            l1_options = sorted(filtered["L1"].dropna().unique().tolist())
            if not l1_options:
                st.info("No categories match your search.")
            else:
                selected_l1 = st.selectbox(
                    "Top-level group (L1)",
                    ["(all)"] + l1_options,
                    key="cat_tree_l1",
                )
                sub = filtered if selected_l1 == "(all)" else filtered[filtered["L1"] == selected_l1]

                if selected_l1 != "(all)" and "L2" in sub.columns:
                    l2_options = sorted(sub["L2"].dropna().unique().tolist())
                    selected_l2 = st.selectbox(
                        "Sub-group (L2)",
                        ["(all)"] + l2_options,
                        key="cat_tree_l2",
                    )
                    if selected_l2 != "(all)":
                        sub = sub[sub["L2"] == selected_l2]

                        if "L3" in sub.columns:
                            l3_options = sorted(sub["L3"].dropna().unique().tolist())
                            if l3_options:
                                selected_l3 = st.selectbox(
                                    "Category (L3)",
                                    ["(all)"] + l3_options,
                                    key="cat_tree_l3",
                                )
                                if selected_l3 != "(all)":
                                    sub = sub[sub["L3"] == selected_l3]

                st.caption(f"{len(sub)} categor{'y' if len(sub)==1 else 'ies'} in selection")

                # Show results as an expandable list grouped by L1 > L2
                level_cols = [c for c in [f"L{i+1}" for i in range(n_levels)] if c in sub.columns]
                for l1_val, grp_l1 in sub.groupby("L1", sort=True):
                    with st.expander(f"{l1_val}  ({len(grp_l1)})", expanded=(len(l1_options) == 1 or bool(q_lower))):
                        if "L2" in grp_l1.columns:
                            for l2_val, grp_l2 in grp_l1.groupby("L2", sort=True, dropna=False):
                                l2_label = str(l2_val) if pd.notna(l2_val) else "(no sub-group)"
                                st.markdown(f"**{l2_label}** — {len(grp_l2)} item(s)")
                                rows_md = []
                                for _, r in grp_l2.iterrows():
                                    deeper = " / ".join(
                                        str(r[c]) for c in level_cols[2:]
                                        if pd.notna(r.get(c)) and str(r.get(c)).strip()
                                    )
                                    label = deeper if deeper else r["Category Name"]
                                    rows_md.append(f"- `{r['Export Code']}` &nbsp; {label}")
                                st.markdown("\n".join(rows_md), unsafe_allow_html=True)
                        else:
                            rows_md = [f"- `{r['Export Code']}` &nbsp; {r['Category Name']}" for _, r in grp_l1.iterrows()]
                            st.markdown("\n".join(rows_md), unsafe_allow_html=True)

        # ── FLAT TABLE VIEW ───────────────────────────────────────────────────
        else:
            level_cols = [c for c in [f"L{i+1}" for i in range(n_levels)] if c in filtered.columns]
            display_df = pd.concat([filtered[level_cols], filtered[["Export Code"]]], axis=1)
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=480,
                column_config={
                    col: st.column_config.TextColumn(col, width="medium")
                    for col in display_df.columns
                },
            )

        st.markdown("---")
        # Download full category list
        cat_out = io.BytesIO()
        with pd.ExcelWriter(cat_out, engine="openpyxl") as w:
            cat_display[["Full Path", "Export Code", "Category Name"]].to_excel(w, index=False, sheet_name="Categories")
        st.download_button(
            "Download full category list (.xlsx)",
            data=cat_out.getvalue(),
            file_name="decathlon_categories.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# =============================================================================
# RESULTS
# =============================================================================

if queries:
    st.markdown("---")
    all_result_frames = []
    no_match          = []

    for q in queries:
        res = search(q)
        if res.empty:
            no_match.append(q)
        else:
            res.insert(0, "Search Term", q)
            all_result_frames.append((q, res))

    if no_match:
        st.warning(f"No matches found for: **{', '.join(no_match)}**")

    if all_result_frames:
        total_rows = sum(len(r) for _, r in all_result_frames)
        st.success(f"**{total_rows} rows** matched across **{len(all_result_frames)}** query(ies)")

        combined = pd.concat([r for _, r in all_result_frames], ignore_index=True)

        # ── 1. Category matching ──────────────────────────────────────────────
        ai_categories = None

        if df_cat is not None and use_ai_matching and zuma_api_key:
            n               = len(combined)
            unique_models_n = combined["model_code"].nunique() if "model_code" in combined.columns else n
            est             = max(2, unique_models_n // concurrency + 2)
            with st.spinner(f"AI category matching {unique_models_n} unique models (~{est}s)…"):
                try:
                    ai_categories, _model_cats = ai_match_categories(
                        combined, leaves, vectorizer, tfidf_matrix, path_to_export,
                        zuma_api_key, zuma_model, shortlist_k, concurrency,
                    )
                    st.success(f"AI matched {unique_models_n} models → {n} SKUs")
                except Exception as e:
                    st.error(f"Zuma AI category error: {e}")
                    use_ai_matching = False
        elif df_cat is not None and use_ai_matching and not zuma_api_key:
            st.warning("Enter your Zuma AI API key in the sidebar to use AI matching.")
            use_ai_matching = False

        # ── 2. Short descriptions ─────────────────────────────────────────────
        short_descs = None

        if use_ai_matching and ai_short_desc and zuma_api_key:
            with st.spinner(f"Generating AI short descriptions ({len(combined)} products)…"):
                try:
                    short_descs = ai_short_descriptions(combined, zuma_api_key, zuma_model, concurrency)
                    st.success("Short descriptions generated")
                except Exception as e:
                    st.error(f"Short desc error: {e}")
                    short_descs = None

        if short_descs is None:
            short_descs = [rule_based_short_desc(row) for _, row in combined.iterrows()]

        # ── 3. Category path lookup ───────────────────────────────────────────
        if df_cat is not None:
            _exp_to_path: dict = {}
            for _, _rc in df_cat.iterrows():
                _e = str(_rc.get("export_category", "")).strip()
                _p = str(_rc.get("Category Path", "")).strip()
                if _e and _p and _e not in _exp_to_path:
                    _exp_to_path[_e] = _p
        else:
            _exp_to_path = {}

        # Formats the preview category to explicitly show "CODE - PATH"
        def _code_to_full(code):
            c = str(code).strip()
            if not c:
                return ""
            p = _exp_to_path.get(c, c)
            return f"{c} - {p}" if c != p else p

        # ── 4. Compute auto variations & categories for preview ───────────────
        if "size_overrides" not in st.session_state:
            st.session_state.size_overrides = {}

        preview = combined.copy()
        preview.reset_index(drop=True, inplace=True)

        if ai_categories:
            preview["_primary_cat"] = [_code_to_full(c[0]) for c in ai_categories]
        elif df_cat is not None:
            kw = keyword_match_batch(preview, df_cat)
            preview["_primary_cat"] = [_code_to_full(c[0]) for c in kw]
        else:
            preview["_primary_cat"] = ""

        # Compute variation using per-row overrides
        def _compute_var(row):
            override = st.session_state.size_overrides.get(row.name)
            return get_variation(
                row,
                is_fashion=is_fashion,
                valid_sizes=valid_sizes,
                size_override=override,
            )

        preview["_variation"] = preview.apply(_compute_var, axis=1)
        preview["_size_ok"]   = preview["_variation"].apply(
            lambda v: not is_size_missing(v, valid_sizes) if is_fashion else True
        )

        # ── 5. Preview table — final export look ──────────────────────────────
        st.markdown("---")
        st.subheader(f"Export Preview — {total_rows} SKU(s)")

        if is_fashion:
            st.info(
                "**Fashion mode** — rows highlighted in red have a size not found in sizes.txt. "
                "Use the dropdowns below to fix them before downloading."
            )
        else:
            st.info("**Other mode** — variation is taken from the size column; '...' shown when empty.")

        # Build display columns matching export template
        def _export_name(row):
            pn    = str(row.get("product_name", "")).strip()
            col   = str(row.get("color", "")).split("|")[0].strip()
            if pn and col and col.lower() not in pn.lower():
                return f"{pn} - {col.title()}"
            return pn

        preview["_export_name"] = preview.apply(_export_name, axis=1)

        # Explicitly separate the column names so you know exactly which mode is active
        if is_fashion:
            preview["Size"] = preview["_variation"]
            display_cols = ["sku_num_sku_r3", "_export_name", "color", "Size", "_primary_cat", "brand_name", "bar_code", "_size_ok"]
        else:
            preview["Variation"] = preview["_variation"]
            display_cols = ["sku_num_sku_r3", "_export_name", "color", "Variation", "_primary_cat", "brand_name", "bar_code", "_size_ok"]

        show_cols = [c for c in display_cols if c in preview.columns]

        col_cfg = {
            "sku_num_sku_r3":   st.column_config.TextColumn("SKU",                width="small"),
            "_export_name":     st.column_config.TextColumn("Name (export)",      width="large"),
            "color":            st.column_config.TextColumn("Colour",             width="medium"),
            "Size":             st.column_config.TextColumn("Size (Export)",      width="medium"),
            "Variation":        st.column_config.TextColumn("Variation (Export)", width="medium"),
            "_primary_cat":     st.column_config.TextColumn("Primary Category",   width="large"),
            "brand_name":       st.column_config.TextColumn("Brand",              width="small"),
            "bar_code":         st.column_config.TextColumn("Barcode",            width="medium"),
            "_size_ok":         st.column_config.CheckboxColumn("Size OK",        width="small"),
        }

        df_display = preview[show_cols].copy()
        if is_fashion and "_size_ok" in df_display.columns:
            df_display.insert(0, "⚠️", df_display["_size_ok"].apply(
                lambda ok: "" if ok else "⚠️ fix size"
            ))
            col_cfg["⚠️"] = st.column_config.TextColumn("", width="small")

        st.dataframe(df_display, use_container_width=True, hide_index=True, height=420,
                     column_config=col_cfg)

        # ── 6. Per-row size fix (fashion only) ────────────────────────────────
        if is_fashion and valid_sizes:
            bad_rows = preview[~preview["_size_ok"]]
            if not bad_rows.empty:
                st.markdown("---")
                st.subheader(f"Fix Sizes — {len(bad_rows)} row(s) need attention")
                st.caption("Select the correct size for each flagged row. Changes apply to the downloaded template.")

                for pos_idx, row in bad_rows.iterrows():
                    sku   = row.get("sku_num_sku_r3", f"row {pos_idx}")
                    name  = str(row.get("product_name", ""))[:50]
                    raw_s = str(row.get("size", ""))
                    current_override = st.session_state.size_overrides.get(pos_idx)
                    current_val      = current_override or row["_variation"]

                    opts = ["(auto)"] + valid_sizes
                    try:
                        sel_idx = opts.index(current_val) if current_val in opts else 0
                    except ValueError:
                        sel_idx = 0

                    col1, col2, col3 = st.columns([2, 2, 3])
                    col1.markdown(f"**{sku}** \n`{name}`")
                    col2.markdown(f"Master: `{raw_s}`  \nAuto: `{row['_variation']}`")
                    chosen = col3.selectbox(
                        f"Size for {sku}",
                        opts,
                        index=sel_idx,
                        key=f"size_fix_{pos_idx}",
                        label_visibility="collapsed",
                    )
                    if chosen != "(auto)":
                        st.session_state.size_overrides[pos_idx] = chosen
                    elif pos_idx in st.session_state.size_overrides:
                        del st.session_state.size_overrides[pos_idx]

                if st.button("Re-apply size fixes to preview"):
                    st.rerun()
            else:
                st.success("All sizes matched in sizes.txt")

        # ── 7. Category editor ────────────────────────────────────────────────
        if df_cat is not None:
            st.markdown("---")
            mode_label = "AI" if (use_ai_matching and ai_categories) else "Keyword"
            st.subheader(f"Category Editor — {mode_label}")
            st.caption(
                "Categories are shared across all SKUs with the same model code. "
                "Edit one row per model — siblings update automatically on export."
            )

            export_to_path: dict = {}
            for _, row_c in df_cat.iterrows():
                exp  = str(row_c.get("export_category", "")).strip()
                path = str(row_c.get("Category Path", "")).strip()
                if exp and path and exp not in export_to_path:
                    export_to_path[exp] = path
            path_label_to_export: dict = {v: k for k, v in export_to_path.items()}

            def export_to_label(code: str) -> str:
                if not code:
                    return ""
                return export_to_path.get(code, code)

            def label_to_export(label: str) -> str:
                if not label or label == "(auto)":
                    return ""
                return path_label_to_export.get(label, label)

            all_path_labels    = sorted(export_to_path.values())
            all_labels_w_blank = ["(auto)"] + all_path_labels

            if "cat_overrides" not in st.session_state:
                st.session_state.cat_overrides = {}

            cat_search = st.text_input(
                "Filter category list",
                placeholder="e.g. football, running, kids...",
                key="cat_search",
            )
            q_cat = cat_search.strip().lower()
            filtered_labels = (
                ["(auto)"] + [lbl for lbl in all_path_labels if q_cat in lbl.lower()]
                if q_cat else all_labels_w_blank
            )
            st.caption(
                f"{len(filtered_labels)-1} categories shown"
                + (f" matching '{cat_search}'" if q_cat else " (all)")
                + f" · {len(st.session_state.cat_overrides)} model override(s)"
            )
            st.markdown("---")

            seen_models: set = set()
            hc1, hc2, hc4 = st.columns([2, 5, 1])
            hc1.markdown("**Model · SKUs**")
            hc2.markdown("**Primary Category**")
            hc4.markdown("**Method**")

            for i, (_, prow) in enumerate(combined.iterrows()):
                mc = str(prow.get("model_code", "")).strip()
                if mc in seen_models:
                    continue
                seen_models.add(mc)

                sku_count = len(combined[combined["model_code"] == mc])
                name      = str(prow.get("product_name", ""))[:40]

                if ai_categories:
                    first_idx = next(
                        j for j, (_, r) in enumerate(combined.iterrows())
                        if str(r.get("model_code", "")).strip() == mc
                    )
                    auto_prim_code, _ = ai_categories[first_idx]
                else:
                    auto_prim_code, _ = keyword_match_category(prow, df_cat)

                auto_prim_label = export_to_label(auto_prim_code)
                override        = st.session_state.cat_overrides.get(mc, {})
                cur_prim_label  = export_to_label(override.get("primary", auto_prim_code)) or auto_prim_label

                c1, c2, c4 = st.columns([2, 5, 1])
                c1.markdown(f"**{mc}** \n{name} \n`{sku_count} SKU(s)`")

                prim_opts = (
                    filtered_labels if cur_prim_label in filtered_labels
                    else ["(auto)", cur_prim_label] + [l for l in filtered_labels if l != "(auto)"]
                )
                try:    prim_idx = prim_opts.index(cur_prim_label)
                except ValueError: prim_idx = 0

                new_prim_label = c2.selectbox(
                    f"Primary #{mc}", prim_opts,
                    index=prim_idx, label_visibility="collapsed", key=f"prim_{mc}",
                )
                new_prim_code = label_to_export(new_prim_label) if new_prim_label != "(auto)" else auto_prim_code

                if new_prim_label != "(auto)":
                    st.session_state.cat_overrides[mc] = {"primary": new_prim_code, "additional": ""}
                elif mc in st.session_state.cat_overrides:
                    del st.session_state.cat_overrides[mc]

                badge = "Manual" if mc in st.session_state.cat_overrides else (
                    "AI" if (use_ai_matching and ai_categories) else "Keyword"
                )
                c4.markdown(f"`{badge}`")

        # ── 8. Download ───────────────────────────────────────────────────────
        st.markdown("---")

        if df_cat is None:
            st.warning("deca_cat.xlsx not loaded — template download unavailable.")
        else:
            try:
                # Merge categories: override > AI > keyword
                merged_cats = []
                for i2, (_, prow) in enumerate(combined.iterrows()):
                    mc2      = str(prow.get("model_code", "")).strip()
                    override = st.session_state.get("cat_overrides", {}).get(mc2)
                    if override:
                        merged_cats.append((override["primary"], override.get("additional", "")))
                    elif ai_categories:
                        first_idx = next(
                            j for j, (_, r) in enumerate(combined.iterrows())
                            if str(r.get("model_code", "")).strip() == mc2
                        )
                        merged_cats.append(ai_categories[first_idx])
                    else:
                        merged_cats.append(keyword_match_category(prow, df_cat))

                # Build index-keyed size overrides for build_template
                idx_overrides = {}
                for pos_idx, sz in st.session_state.size_overrides.items():
                    if pos_idx < len(combined):
                        real_idx = combined.index[pos_idx]
                        idx_overrides[real_idx] = sz

                tpl_bytes = build_template(
                    combined,
                    df_cat,
                    df_brands,
                    ai_categories=merged_cats,
                    short_descs=short_descs,
                    is_fashion=is_fashion,
                    valid_sizes=valid_sizes,
                    size_overrides=idx_overrides,
                )
                mode_icon = "AI" if (use_ai_matching and ai_categories) else "KW"
                st.download_button(
                    f"{mode_icon} Download Filled Upload Template (.xlsx)",
                    data=tpl_bytes,
                    file_name="decathlon_upload_template_filled.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )
            except FileNotFoundError:
                st.warning(
                    "Template file not found. "
                    "Place `product-creation-template.xlsx` in the app folder."
                )

else:
    st.info("Upload a list or type search terms above to get started.")

st.markdown("---")
st.caption("Decathlon Product Lookup · Powered by your Decathlon working file · AI by Zuma")
